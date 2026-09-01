#ifndef DOLTLITE_ENABLE_CHUNK_SOURCE
# define DOLTLITE_ENABLE_CHUNK_SOURCE 1
#endif

#if defined(DOLTLITE_PROLLY) && DOLTLITE_ENABLE_CHUNK_SOURCE

#include "prolly_btree_int.h"
#include "doltlite_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define BASE_ROWS 2400
#define EXTRA_ROWS 100
#define ADVANCE_ROWS 25
#define PAYLOAD_BYTES 700
#define OVERSIZE_BYTES 17825792

#define SOURCE_NORMAL    0
#define SOURCE_NOTFOUND  1
#define SOURCE_CORRUPT   2
#define SOURCE_IOERR     3
#define SOURCE_BATCH_IOERR 4
#define SOURCE_ONE_NOTFOUND 5

typedef struct SourceCtx SourceCtx;
struct SourceCtx {
  ChunkStore store;
  int storeOpen;
  int mode;
  int failAfter;
  int nGet;
  int nGetMany;
  int nManyHash;
  int nRequest;
  int nReturned;
  int faultIssued;
  unsigned char lastHash[PROLLY_HASH_SIZE];
  unsigned char faultHash[PROLLY_HASH_SIZE];
};

static int nPass;
static int nFail;
static int faultCode;

static int chunkSourceFaultCallback(int iCode){
  return iCode==faultCode;
}

static void check(const char *zName, int ok){
  if( ok ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static int execSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error %d: %s\nSQL: %s\n", rc,
            zErr ? zErr : sqlite3_errmsg(db), zSql);
  }
  sqlite3_free(zErr);
  return rc;
}

static int openDb(const char *zPath, int flags, sqlite3 **ppDb){
  int rc = sqlite3_open_v2(zPath, ppDb, flags, 0);
  if( *ppDb ){
    sqlite3_extended_result_codes(*ppDb, 1);
    sqlite3_busy_timeout(*ppDb, 5000);
  }
  return rc;
}

static void removeStore(const char *zPath){
  const char *zBase = strrchr(zPath, '/');
  char zAux[512];
  remove(zPath);
  snprintf(zAux, sizeof(zAux), "%s-wal", zPath);
  remove(zAux);
  if( zBase ){
    int nDir = (int)(zBase - zPath) + 1;
    snprintf(zAux, sizeof(zAux), "%.*s.%s-lock",
             nDir, zPath, zBase + 1);
  }else{
    snprintf(zAux, sizeof(zAux), ".%s-lock", zPath);
  }
  remove(zAux);
}

static int queryInt64(sqlite3 *db, const char *zSql, sqlite3_int64 *pValue){
  sqlite3_stmt *pStmt = 0;
  int rc;
  *pValue = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      *pValue = sqlite3_column_int64(pStmt, 0);
      rc = SQLITE_OK;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_NOTFOUND;
    }
  }
  if( pStmt ){
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && frc!=SQLITE_OK ) rc = frc;
  }
  return rc;
}

static int queryText(sqlite3 *db, const char *zSql, char **pzValue){
  sqlite3_stmt *pStmt = 0;
  int rc;
  *pzValue = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      const unsigned char *z = sqlite3_column_text(pStmt, 0);
      *pzValue = sqlite3_mprintf("%s", z ? (const char*)z : "");
      rc = *pzValue ? SQLITE_OK : SQLITE_NOMEM;
    }else if( rc==SQLITE_DONE ){
      rc = SQLITE_NOTFOUND;
    }
  }
  if( pStmt ){
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && frc!=SQLITE_OK ) rc = frc;
  }
  return rc;
}

static int queryResult(sqlite3 *db, const char *zSql, char **pzOut){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pStr = 0;
  int rc;
  int nCol;
  *pzOut = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  pStr = sqlite3_str_new(db);
  if( !pStr ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOMEM;
  }
  nCol = sqlite3_column_count(pStmt);
  while( (rc=sqlite3_step(pStmt))==SQLITE_ROW ){
    int i;
    sqlite3_str_appendf(pStr, "R%d", nCol);
    for(i=0; i<nCol; i++){
      int eType = sqlite3_column_type(pStmt, i);
      if( eType==SQLITE_NULL ){
        sqlite3_str_appendall(pStr, "|N");
      }else{
        const unsigned char *z = sqlite3_column_text(pStmt, i);
        int n = sqlite3_column_bytes(pStmt, i);
        sqlite3_str_appendf(pStr, "|%d:%d:", eType, n);
        if( n>0 ) sqlite3_str_append(pStr, (const char*)z, n);
      }
    }
    sqlite3_str_appendchar(pStr, 1, '\n');
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc==SQLITE_OK && sqlite3_str_errcode(pStr)!=SQLITE_OK ){
    rc = sqlite3_str_errcode(pStr);
  }
  {
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && frc!=SQLITE_OK ) rc = frc;
  }
  if( rc==SQLITE_OK ){
    *pzOut = sqlite3_str_finish(pStr);
    if( !*pzOut ) rc = SQLITE_NOMEM;
  }else{
    sqlite3_free(sqlite3_str_finish(pStr));
  }
  return rc;
}

static void compareQuery(
  sqlite3 *a,
  sqlite3 *b,
  const char *zName,
  const char *zSql
){
  char *zA = 0;
  char *zB = 0;
  char zLabel[192];
  int rcA = queryResult(a, zSql, &zA);
  int rcB = queryResult(b, zSql, &zB);
  snprintf(zLabel, sizeof(zLabel), "%s executes on source", zName);
  check(zLabel, rcA==SQLITE_OK);
  snprintf(zLabel, sizeof(zLabel), "%s executes on lazy store", zName);
  check(zLabel, rcB==SQLITE_OK);
  snprintf(zLabel, sizeof(zLabel), "%s results match", zName);
  check(zLabel, rcA==SQLITE_OK && rcB==SQLITE_OK
                && zA && zB && strcmp(zA, zB)==0);
  if( rcA!=SQLITE_OK ){
    fprintf(stderr, "  source query error: %s\n  SQL: %s\n",
            sqlite3_errmsg(a), zSql);
  }
  if( rcB!=SQLITE_OK ){
    fprintf(stderr, "  lazy query error: %s\n  SQL: %s\n",
            sqlite3_errmsg(b), zSql);
  }
  sqlite3_free(zA);
  sqlite3_free(zB);
}

static void fillPayload(int iRow, char *zBuf){
  int i;
  int n = snprintf(zBuf, PAYLOAD_BYTES + 1, "row-%06d-", iRow);
  for(i=n; i<PAYLOAD_BYTES; i++){
    zBuf[i] = (char)('a' + ((iRow * 7 + i * 11) % 26));
  }
  zBuf[PAYLOAD_BYTES] = 0;
}

static int insertRows(sqlite3 *db, int iFirst, int nRow){
  sqlite3_stmt *pStmt = 0;
  char zPayload[PAYLOAD_BYTES + 1];
  int rc;
  int i;
  rc = sqlite3_prepare_v2(db,
      "INSERT INTO items(id,grp,payload) VALUES(?1,?2,?3)",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<nRow && rc==SQLITE_OK; i++){
    int iRow = iFirst + i;
    fillPayload(iRow, zPayload);
    sqlite3_bind_int(pStmt, 1, iRow);
    sqlite3_bind_int(pStmt, 2, iRow % 37);
    sqlite3_bind_text(pStmt, 3, zPayload, PAYLOAD_BYTES, SQLITE_TRANSIENT);
    rc = sqlite3_step(pStmt);
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    sqlite3_reset(pStmt);
    sqlite3_clear_bindings(pStmt);
  }
  {
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && frc!=SQLITE_OK ) rc = frc;
  }
  return rc;
}

static int buildSource(const char *zPath, sqlite3 **ppDb){
  sqlite3 *db = 0;
  int rc;
  removeStore(zPath);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  if( rc!=SQLITE_OK ) goto build_done;
  rc = execSql(db,
      "CREATE TABLE items("
      " id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, payload TEXT NOT NULL);"
      "CREATE INDEX items_grp ON items(grp);"
      "CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);"
      "INSERT INTO meta VALUES('fixture','base');"
      "CREATE TABLE oversize_payload("
      " id INTEGER PRIMARY KEY, payload BLOB NOT NULL);"
      "INSERT INTO oversize_payload VALUES(1,zeroblob(17825792));"
      "BEGIN");
  if( rc==SQLITE_OK ) rc = insertRows(db, 1, BASE_ROWS);
  if( rc==SQLITE_OK ) rc = execSql(db, "COMMIT");
  else execSql(db, "ROLLBACK");
  if( rc==SQLITE_OK ){
    rc = execSql(db, "SELECT dolt_commit('-A','-m','base fixture')");
  }
  if( rc==SQLITE_OK ) rc = execSql(db, "SELECT dolt_branch('feature')");
  if( rc==SQLITE_OK ) rc = execSql(db, "SELECT dolt_checkout('feature')");
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "BEGIN;"
      "UPDATE items SET grp=grp+200, payload='feature-'||payload "
      " WHERE id%19=0;"
      "INSERT INTO items VALUES(9001,9,'feature-only');"
      "UPDATE meta SET v='feature';"
      "COMMIT;"
      "SELECT dolt_commit('-A','-m','feature work')");
  }
  if( rc==SQLITE_OK ) rc = execSql(db, "SELECT dolt_checkout('main')");
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "BEGIN;"
      "UPDATE items SET grp=grp+100, payload='main-'||payload "
      " WHERE id%17=0;"
      "DELETE FROM items WHERE id%41=0;"
      "UPDATE meta SET v='main';");
  }
  if( rc==SQLITE_OK ) rc = insertRows(db, 3001, EXTRA_ROWS);
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "COMMIT; SELECT dolt_commit('-A','-m','main work')");
  }else{
    execSql(db, "ROLLBACK");
  }
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "SELECT dolt_remote('add','origin',"
      "'https://example.invalid/doltlite/chunks')");
  }

build_done:
  if( rc==SQLITE_OK ){
    *ppDb = db;
  }else if( db ){
    sqlite3_close(db);
  }
  return rc;
}

static int advanceSource(sqlite3 *db){
  int rc = execSql(db,
      "BEGIN; UPDATE items SET grp=grp+1000, payload='advanced-'||payload "
      "WHERE id=1;");
  if( rc==SQLITE_OK ) rc = insertRows(db, 5001, ADVANCE_ROWS);
  if( rc==SQLITE_OK ){
    rc = execSql(db,
      "COMMIT; SELECT dolt_commit('-A','-m','remote advance')");
  }else{
    execSql(db, "ROLLBACK");
  }
  return rc;
}

static void sourceResetCounters(SourceCtx *p){
  p->nGet = 0;
  p->nGetMany = 0;
  p->nManyHash = 0;
  p->nRequest = 0;
  p->nReturned = 0;
  p->faultIssued = 0;
  memset(p->lastHash, 0, sizeof(p->lastHash));
  memset(p->faultHash, 0, sizeof(p->faultHash));
}

static int sourceOpen(SourceCtx *p, const char *zPath){
  int rc;
  memset(&p->store, 0, sizeof(p->store));
  rc = chunkStoreOpen(&p->store, sqlite3_vfs_find(0), zPath,
      SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  p->storeOpen = rc==SQLITE_OK;
  return rc;
}

static void sourceClose(SourceCtx *p){
  if( p->storeOpen ) chunkStoreClose(&p->store);
  p->storeOpen = 0;
}

static int sourceFetchOne(
  SourceCtx *p,
  const unsigned char *aHash,
  unsigned char **ppBytes,
  int *pnBytes
){
  ProllyHash hash;
  int rc;
  *ppBytes = 0;
  *pnBytes = 0;
  p->nRequest++;
  memcpy(p->lastHash, aHash, PROLLY_HASH_SIZE);
  memcpy(&hash, aHash, sizeof(hash));
  if( p->mode==SOURCE_NOTFOUND ) return DOLTLITE_SOURCE_NOTFOUND;
  if( p->mode==SOURCE_ONE_NOTFOUND
   && memcmp(p->faultHash, aHash, PROLLY_HASH_SIZE)==0 ){
    p->faultIssued = 1;
    return DOLTLITE_SOURCE_NOTFOUND;
  }
  if( p->mode==SOURCE_IOERR && p->nRequest>p->failAfter ){
    p->faultIssued = 1;
    memcpy(p->faultHash, aHash, PROLLY_HASH_SIZE);
    return DOLTLITE_SOURCE_IOERR;
  }
  rc = chunkStoreGet(&p->store, &hash, ppBytes, pnBytes);
  if( rc==SQLITE_NOTFOUND ) return DOLTLITE_SOURCE_NOTFOUND;
  if( rc!=SQLITE_OK ) return DOLTLITE_SOURCE_IOERR;
  p->nReturned++;
  if( p->mode==SOURCE_CORRUPT && !p->faultIssued ){
    if( *pnBytes>0 ) (*ppBytes)[0] ^= 0x80;
    p->faultIssued = 1;
    memcpy(p->faultHash, aHash, PROLLY_HASH_SIZE);
  }
  return DOLTLITE_SOURCE_OK;
}

static int sourceGet(
  void *pCtx,
  const unsigned char aHash[PROLLY_HASH_SIZE],
  unsigned char **ppBytes,
  int *pnBytes
){
  SourceCtx *p = (SourceCtx*)pCtx;
  p->nGet++;
  return sourceFetchOne(p, aHash, ppBytes, pnBytes);
}

static int sourceGetMany(
  void *pCtx,
  int nHash,
  const unsigned char *aHash,
  unsigned char **apBytes,
  int *anBytes
){
  SourceCtx *p = (SourceCtx*)pCtx;
  int i;
  int rc = DOLTLITE_SOURCE_OK;
  p->nGetMany++;
  p->nManyHash += nHash;
  for(i=0; i<nHash; i++){
    apBytes[i] = 0;
    anBytes[i] = 0;
  }
  if( p->mode==SOURCE_NOTFOUND || p->mode==SOURCE_IOERR ){
    return DOLTLITE_SOURCE_OK;
  }
  if( p->mode==SOURCE_BATCH_IOERR ){
    if( nHash<2 ) return DOLTLITE_SOURCE_IOERR;
    rc = sourceFetchOne(p, aHash, &apBytes[0], &anBytes[0]);
    if( rc!=DOLTLITE_SOURCE_OK ) return rc;
    p->nRequest++;
    p->faultIssued = 1;
    memcpy(p->lastHash, aHash + PROLLY_HASH_SIZE, PROLLY_HASH_SIZE);
    memcpy(p->faultHash, aHash + PROLLY_HASH_SIZE, PROLLY_HASH_SIZE);
    return DOLTLITE_SOURCE_IOERR;
  }
  for(i=0; i<nHash; i++){
    rc = sourceFetchOne(p, aHash + i*PROLLY_HASH_SIZE,
                        &apBytes[i], &anBytes[i]);
    if( rc==DOLTLITE_SOURCE_NOTFOUND ){
      rc = DOLTLITE_SOURCE_OK;
      continue;
    }
    if( rc!=DOLTLITE_SOURCE_OK ) return rc;
  }
  return DOLTLITE_SOURCE_OK;
}

static void initSourceApi(SourceCtx *p, doltlite_chunk_source *pApi){
  memset(pApi, 0, sizeof(*pApi));
  pApi->iVersion = 1;
  pApi->pCtx = p;
  pApi->xGet = sourceGet;
  pApi->xGetMany = sourceGetMany;
}

static int serializeRefs(SourceCtx *p, unsigned char **ppRefs, int *pnRefs){
  *ppRefs = 0;
  *pnRefs = 0;
  return chunkStoreSerializeRefsToBlob(&p->store, ppRefs, pnRefs);
}

static int createLazyFile(
  const char *zPath,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  int rc;
  removeStore(zPath);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  if( rc==SQLITE_OK ) rc = doltlite_init_lazy(db, pRefs, nRefs);
  if( db ){
    int crc = sqlite3_close(db);
    if( rc==SQLITE_OK && crc!=SQLITE_OK ) rc = crc;
  }
  return rc;
}

static int collectMissingHashes(
  ChunkStore *pDest,
  const ChunkIndexEntry *aEntry,
  int nEntry,
  ProllyHash *aOut,
  int nWant,
  int *pnOut
){
  int rc = SQLITE_OK;
  int i;
  for(i=0; i<nEntry && *pnOut<nWant; i++){
    int has = 0;
    rc = chunkStoreHas(pDest, &aEntry[i].hash, &has);
    if( rc!=SQLITE_OK ) return rc;
    if( !has ) aOut[(*pnOut)++] = aEntry[i].hash;
  }
  return SQLITE_OK;
}

static int findMissingHashes(
  sqlite3 *db,
  SourceCtx *pCtx,
  ProllyHash *aOut,
  int nWant
){
  Btree *pBtree = db->aDb[0].pBt;
  ChunkStore *pDest;
  const ChunkIndexEntry *aEntry = 0;
  int nEntry = 0;
  int nOut = 0;
  int rc;
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    return SQLITE_NOTFOUND;
  }
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  pDest = doltliteBtreeChunkStore(pBtree);
  rc = csMaterializeIndex(&pCtx->store);
  if( rc==SQLITE_OK ){
    chunkIndexGetEntries(&pCtx->store.index, &nEntry, &aEntry);
    rc = collectMissingHashes(
        pDest, aEntry, nEntry, aOut, nWant, &nOut);
  }
  if( rc==SQLITE_OK && nOut<nWant ){
    chunkStagingGetRecent(&pCtx->store.staging, &nEntry, &aEntry);
    rc = collectMissingHashes(
        pDest, aEntry, nEntry, aOut, nWant, &nOut);
  }
  if( rc==SQLITE_OK && nOut<nWant ){
    chunkStagingGetPending(&pCtx->store.staging, &nEntry, &aEntry);
    rc = collectMissingHashes(
        pDest, aEntry, nEntry, aOut, nWant, &nOut);
  }
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  if( rc==SQLITE_OK && nOut<nWant ) rc = SQLITE_NOTFOUND;
  return rc;
}

static int getDbChunk(
  sqlite3 *db,
  const ProllyHash *pHash,
  unsigned char **ppData,
  int *pnData
){
  Btree *pBtree = db->aDb[0].pBt;
  ChunkStore *pStore;
  int rc;
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    return SQLITE_NOTFOUND;
  }
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  pStore = doltliteBtreeChunkStore(pBtree);
  rc = chunkStoreGet(pStore, pHash, ppData, pnData);
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

static int resetTreeCache(sqlite3 *db){
  Btree *pBtree = db->aDb[0].pBt;
  int rc;
  if( !pBtree || !sqlite3BtreeIsDoltliteFormat(pBtree) ){
    return SQLITE_NOTFOUND;
  }
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  prollyCacheFree(&pBtree->pBt->cache);
  rc = prollyCacheInit(&pBtree->pBt->cache, PROLLY_DEFAULT_CACHE_SIZE);
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

static int runToError(sqlite3 *db, const char *zSql, char **pzErr){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  *pzErr = 0;
  if( rc==SQLITE_OK ){
    do {
      rc = sqlite3_step(pStmt);
    } while( rc==SQLITE_ROW );
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
  if( pStmt ) sqlite3_finalize(pStmt);
  return rc;
}

static void testNoSourceAncestorError(sqlite3 *db){
  char *zErr = 0;
  int rc = runToError(db, "SELECT dolt_hashof('HEAD~999')", &zErr);
  check("no-source past-root ancestor remains an error", rc==SQLITE_ERROR);
  check("no-source past-root keeps ancestor diagnostic",
        zErr && strstr(zErr, "invalid ancestor spec")!=0);
  sqlite3_free(zErr);
}

static void hashHex(const unsigned char *aHash, char zOut[41]){
  static const char zHex[] = "0123456789abcdef";
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    zOut[i*2] = zHex[aHash[i] >> 4];
    zOut[i*2+1] = zHex[aHash[i] & 0x0f];
  }
  zOut[40] = 0;
}

static void runOracleComparisons(sqlite3 *a, sqlite3 *b){
  compareQuery(a, b, "point rows",
      "SELECT id,grp,payload FROM items "
      "WHERE id IN (1,17,999,2399,3001) ORDER BY id");
  compareQuery(a, b, "full table scan",
      "SELECT id,grp,payload FROM items ORDER BY id");
  compareQuery(a, b, "full zero-tail chunk",
      "SELECT id,length(payload) FROM oversize_payload");
  compareQuery(a, b, "dolt_log",
      "SELECT commit_hash,committer,date,message FROM dolt_log");
  compareQuery(a, b, "dolt_branches",
      "SELECT name,hash FROM dolt_branches ORDER BY name");
  compareQuery(a, b, "dolt_remotes",
      "SELECT name,url FROM dolt_remotes ORDER BY name");
  compareQuery(a, b, "dolt_diff_stat",
      "SELECT table_name,rows_unmodified,rows_added,rows_deleted,"
      "rows_modified,cells_added,cells_deleted,cells_modified "
      "FROM dolt_diff_stat('HEAD~1','HEAD','items') ORDER BY table_name");
  compareQuery(a, b, "dolt_at_items",
      "SELECT id,grp,payload FROM dolt_at_items('feature') ORDER BY id");
}

static void testOracleWriteThroughAndRefresh(
  const char *zSource,
  const char *zLazy,
  sqlite3 *a,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs,
  unsigned char **ppNewRefs,
  int *pnNewRefs
){
  sqlite3 *b = 0;
  char *zOldHead = 0;
  char *zOldBefore = 0;
  char *zOldAfter = 0;
  char zOldSql[256];
  int rc;
  int nCallback;

  zOldSql[0] = 0;

  check("create writable refs-only store",
        createLazyFile(zLazy, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zLazy, SQLITE_OPEN_READWRITE, &b);
  check("reopen writable refs-only store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto oracle_done;
  rc = doltlite_set_chunk_source(b, "main", pApi);
  check("register source on writable store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto oracle_done;

  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  runOracleComparisons(a, b);
  nCallback = pCtx->nGet + pCtx->nGetMany;
  check("writable scan uses xGetMany", pCtx->nGetMany>0);
  check("xGetMany receives multiple child hashes",
        pCtx->nManyHash>pCtx->nGetMany);
  check("batched callbacks are well below scanned rows",
        nCallback>0 && nCallback<BASE_ROWS/16);
  check("batched callbacks are well below fetched chunks",
        nCallback>0 && nCallback*4<pCtx->nReturned);

  rc = queryText(a,
      "SELECT commit_hash FROM dolt_log LIMIT 1", &zOldHead);
  check("capture old source head", rc==SQLITE_OK && zOldHead!=0);
  if( rc==SQLITE_OK ){
    snprintf(zOldSql, sizeof(zOldSql),
      "SELECT id,grp,payload FROM dolt_at_items('%s') "
      "WHERE id IN (1,17,2399) ORDER BY id", zOldHead);
    rc = queryResult(b, zOldSql, &zOldBefore);
    check("cache old-head rows before refs refresh", rc==SQLITE_OK);
  }

  sourceClose(pCtx);
  rc = advanceSource(a);
  check("advance source repository", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = sourceOpen(pCtx, zSource);
  check("reopen source chunk store after advance", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = serializeRefs(pCtx, ppNewRefs, pnNewRefs);
  check("serialize advanced refs", rc==SQLITE_OK && *ppNewRefs!=0);
  if( rc==SQLITE_OK ){
    rc = doltlite_init_lazy(b, *ppNewRefs, *pnNewRefs);
    check("install updated refs on live lazy store", rc==SQLITE_OK);
  }
  if( rc==SQLITE_OK ){
    compareQuery(a, b, "advanced head rows",
      "SELECT id,grp,payload FROM items "
      "WHERE id IN (1,5001,5025) ORDER BY id");
    compareQuery(a, b, "advanced head log",
      "SELECT commit_hash,message FROM dolt_log");
    if( zOldBefore ){
      rc = queryResult(b, zOldSql, &zOldAfter);
      check("read old cached head after refs refresh", rc==SQLITE_OK);
      check("old cached head is unchanged",
            zOldAfter && strcmp(zOldBefore, zOldAfter)==0);
    }
    compareQuery(a, b, "advanced full scan before source clear",
      "SELECT id,grp,payload FROM items ORDER BY id");
  }

  rc = doltlite_set_chunk_source(b, "main", 0);
  check("clear writable chunk source", rc==SQLITE_OK);
  sqlite3_close(b);
  b = 0;
  nCallback = pCtx->nGet + pCtx->nGetMany;
  rc = openDb(zLazy, SQLITE_OPEN_READWRITE, &b);
  check("reopen write-through store without source", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    compareQuery(a, b, "write-through full scan",
      "SELECT id,grp,payload FROM items ORDER BY id");
    compareQuery(a, b, "write-through full zero-tail chunk",
      "SELECT id,length(payload) FROM oversize_payload");
    check("local read does not call cleared source",
          nCallback==pCtx->nGet+pCtx->nGetMany);
  }

oracle_done:
  sqlite3_free(zOldHead);
  sqlite3_free(zOldBefore);
  sqlite3_free(zOldAfter);
  if( b ) sqlite3_close(b);
}

static void testReadOnlyCacheAndBatching(
  const char *zPath,
  sqlite3 *a,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *b = 0;
  sqlite3_int64 want = 0;
  sqlite3_int64 got = 0;
  sqlite3_int64 again = 0;
  sqlite3_int64 oversize = 0;
  int rc;
  int nGet;
  int nMany;
  int nCalls;
  const char *zScan =
      "SELECT sum(id+grp+length(payload)) FROM items WHERE payload IS NOT NULL";

  check("create read-only refs-only store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &b);
  check("reopen refs-only store read-only", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto readonly_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(b, "main", pApi);
  check("register source on read-only store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto readonly_done;
  rc = queryInt64(a, zScan, &want);
  check("source aggregate scan succeeds", rc==SQLITE_OK);
  rc = queryInt64(b, zScan, &got);
  check("lazy read-only aggregate scan succeeds", rc==SQLITE_OK);
  check("read-only aggregate matches source", got==want);
  nGet = pCtx->nGet;
  nMany = pCtx->nGetMany;
  nCalls = nGet + nMany;
  check("read-only scan invokes xGetMany", nMany>0);
  check("read-only xGetMany batches child hashes",
        pCtx->nManyHash>nMany);
  check("read-only callbacks are materially below row count",
        nCalls>0 && nCalls<BASE_ROWS/16);
  check("read-only callbacks are materially below fetched chunks",
        nCalls>0 && nCalls*4<pCtx->nReturned);
  rc = resetTreeCache(b);
  check("drop ordinary prolly cache", rc==SQLITE_OK);
  rc = queryInt64(b, zScan, &again);
  check("repeat read-only scan succeeds", rc==SQLITE_OK);
  check("repeat read-only result is stable", again==got);
  check("repeat scan is served by source memory cache",
        pCtx->nGet==nGet && pCtx->nGetMany==nMany);
  rc = queryInt64(
      b, "SELECT length(payload) FROM oversize_payload WHERE id=1", &oversize);
  check("read-only oversized chunk succeeds",
        rc==SQLITE_OK && oversize==OVERSIZE_BYTES);
  nGet = pCtx->nGet;
  nMany = pCtx->nGetMany;
  rc = resetTreeCache(b);
  check("drop prolly cache after oversized read", rc==SQLITE_OK);
  oversize = 0;
  rc = queryInt64(
      b, "SELECT length(payload) FROM oversize_payload WHERE id=1", &oversize);
  check("repeat oversized read succeeds",
        rc==SQLITE_OK && oversize==OVERSIZE_BYTES);
  check("oversized chunk remains in bounded source cache",
        pCtx->nGet==nGet && pCtx->nGetMany==nMany);

readonly_done:
  if( b ) sqlite3_close(b);
}

static void testFailedRegistrationDetaches(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  ProllyHash missing;
  unsigned char *pData = 0;
  int nData = 0;
  int nCall;
  int nRequest;
  int rc;
  memset(&missing, 0, sizeof(missing));
  check("create failed-registration lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open failed-registration lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto detach_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NOTFOUND;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("deferred registration reports source miss",
        (rc & 0xff)==SQLITE_NOTFOUND);
  check("failed registration invoked source", pCtx->nRequest>0);
  if( pCtx->nRequest==0 ) goto detach_done;
  memcpy(missing.data, pCtx->lastHash, sizeof(missing.data));
  nCall = pCtx->nGet + pCtx->nGetMany;
  nRequest = pCtx->nRequest;
  rc = getDbChunk(db, &missing, &pData, &nData);
  check("failed registration leaves no source",
        (rc & 0xff)==SQLITE_NOTFOUND);
  check("detached callback is not called by direct chunk get",
        pCtx->nGet+pCtx->nGetMany==nCall && pCtx->nRequest==nRequest);

detach_done:
  sqlite3_free(pData);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testWorkingSetNotFound(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  ProllyHash workingSet;
  const char *zBranch;
  char zHash[41];
  int rc;
  memset(&workingSet, 0, sizeof(workingSet));
  check("create working-set-miss lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open working-set-miss lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto ws_miss_done;
  zBranch = chunkStoreGetDefaultBranch(&pCtx->store);
  if( !zBranch ) zBranch = "main";
  rc = chunkStoreGetBranchWorkingSet(&pCtx->store, zBranch, &workingSet);
  check("source refs name a working-set chunk",
        rc==SQLITE_OK && !prollyHashIsEmpty(&workingSet));
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&workingSet) ) goto ws_miss_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, workingSet.data, PROLLY_HASH_SIZE);
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("referenced working-set miss is not treated as absent",
        (rc & 0xff)==SQLITE_NOTFOUND && pCtx->faultIssued);
  hashHex(workingSet.data, zHash);
  check("working-set miss names the referenced hash",
        strstr(sqlite3_errmsg(db), zHash)!=0);

ws_miss_done:
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testOtherBranchWorkingSetNotFound(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  Btree *pBtree = 0;
  ChunkStore *cs = 0;
  ProllyHash workingSet;
  ProllyHash savedStaged;
  char zHash[41];
  char *zErr = 0;
  int active = 1;
  int stateUnchanged = 0;
  int rc;
  memset(&workingSet, 0, sizeof(workingSet));
  memset(&savedStaged, 0, sizeof(savedStaged));
  check("create other-branch working-set-miss store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open other-branch working-set-miss store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto other_ws_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register other-branch working-set source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto other_ws_done;
  rc = chunkStoreGetBranchWorkingSet(
      &pCtx->store, "feature", &workingSet);
  check("other branch refs name a working-set chunk",
        rc==SQLITE_OK && !prollyHashIsEmpty(&workingSet));
  if( rc!=SQLITE_OK || prollyHashIsEmpty(&workingSet) ) goto other_ws_done;

  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, workingSet.data, PROLLY_HASH_SIZE);
  pBtree = sqlite3DbNameToBtree(db, "main");
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  cs = doltliteBtreeChunkStore(pBtree);
  rc = doltliteBranchWorkingSetIsRebasing(db, "feature", &active);
  zErr = chunkStoreSourceTakeError(cs, 0);
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  check("other-branch rebase probe propagates referenced miss",
        (rc & 0xff)==SQLITE_NOTFOUND && pCtx->faultIssued);
  hashHex(workingSet.data, zHash);
  check("other-branch rebase miss names the referenced hash",
        zErr && strstr(zErr, zHash)!=0);
  sqlite3_free(zErr);
  zErr = 0;

  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, workingSet.data, PROLLY_HASH_SIZE);
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  savedStaged = pBtree->vc.stagedCatalog;
  rc = doltliteLoadWorkingSet(db, "feature");
  stateUnchanged = prollyHashCompare(
      &savedStaged, &pBtree->vc.stagedCatalog)==0;
  zErr = chunkStoreSourceTakeError(cs, 0);
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  check("other-branch working-set load propagates referenced miss",
        (rc & 0xff)==SQLITE_NOTFOUND && pCtx->faultIssued);
  check("failed other-branch working-set load preserves session state",
        stateUnchanged);
  check("other-branch load miss names the referenced hash",
        zErr && strstr(zErr, zHash)!=0);

other_ws_done:
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testGraphLockMiss(
  const char *zPath,
  const char *zAuxPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3 *auxDb = 0;
  Btree *pAux = 0;
  ChunkStore *pAuxStore = 0;
  ProllyHash missing;
  unsigned char *pData = 0;
  char *zAttach = 0;
  int nData = 0;
  int auxLocked = 0;
  int inTxn = 0;
  int rc;
  memset(&missing, 0, sizeof(missing));
  removeStore(zAuxPath);
  rc = openDb(zAuxPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &auxDb);
  check("create graph-lock peer database", rc==SQLITE_OK);
  if( auxDb ) sqlite3_close(auxDb);
  auxDb = 0;
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  check("create graph-lock lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &db);
  check("open graph-lock lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register graph-lock source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  rc = findMissingHashes(db, pCtx, &missing, 1);
  check("find graph-lock source miss", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  rc = execSql(db, "BEGIN IMMEDIATE");
  check("begin graph-lock transaction", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  inTxn = 1;
  sourceResetCounters(pCtx);
  rc = getDbChunk(db, &missing, &pData, &nData);
  check("graph-lock miss returns busy", (rc & 0xff)==SQLITE_BUSY);
  check("graph-lock miss does not invoke callback",
        pCtx->nGet==0 && pCtx->nGetMany==0 && pCtx->nRequest==0);
  sqlite3_free(pData);
  pData = 0;
  rc = execSql(db, "ROLLBACK");
  check("rollback graph-lock transaction", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) inTxn = 0;
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  rc = getDbChunk(db, &missing, &pData, &nData);
  check("source miss succeeds after graph-lock rollback", rc==SQLITE_OK);
  check("post-rollback miss invokes source", pCtx->nRequest>0);
  sqlite3_free(pData);
  pData = 0;
  zAttach = sqlite3_mprintf("ATTACH %Q AS aux", zAuxPath);
  rc = zAttach ? execSql(db, zAttach) : SQLITE_NOMEM;
  sqlite3_free(zAttach);
  zAttach = 0;
  check("attach graph-lock peer database", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  rc = findMissingHashes(db, pCtx, &missing, 1);
  check("find miss for cross-database graph lock", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  pAux = sqlite3DbNameToBtree(db, "aux");
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pAux);
  pAuxStore = doltliteBtreeChunkStore(pAux);
  rc = pAuxStore ? chunkStoreLockAndRefresh(pAuxStore) : SQLITE_ERROR;
  if( rc==SQLITE_OK ) auxLocked = 1;
  sqlite3BtreeLeave(pAux);
  sqlite3_mutex_leave(db->mutex);
  check("hold graph lock on another attached database", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto graph_lock_done;
  sourceResetCounters(pCtx);
  rc = getDbChunk(db, &missing, &pData, &nData);
  check("cross-database graph-lock miss returns busy",
        (rc & 0xff)==SQLITE_BUSY);
  check("cross-database graph lock suppresses callback",
        pCtx->nGet==0 && pCtx->nGetMany==0 && pCtx->nRequest==0);

graph_lock_done:
  sqlite3_free(pData);
  sqlite3_free(zAttach);
  if( inTxn ) execSql(db, "ROLLBACK");
  if( auxLocked ){
    sqlite3_mutex_enter(db->mutex);
    sqlite3BtreeEnter(pAux);
    chunkStoreUnlock(pAuxStore);
    sqlite3BtreeLeave(pAux);
    sqlite3_mutex_leave(db->mutex);
  }
  if( db ){
    doltlite_set_chunk_source(db, "main", 0);
    sqlite3_close(db);
  }
  removeStore(zAuxPath);
  pCtx->mode = SOURCE_NORMAL;
}

static void testBatchPartialIoerr(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  Btree *pBtree;
  ChunkStore *pStore;
  ChunkStore victim;
  ProllyHash aMissing[2];
  char zHash[41];
  char *zErr = 0;
  int victimOpen = 0;
  int has = 0;
  int rc;
  memset(&victim, 0, sizeof(victim));
  memset(aMissing, 0, sizeof(aMissing));
  check("create partial-batch lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &db);
  check("open partial-batch lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto batch_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register partial-batch source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto batch_done;
  rc = findMissingHashes(db, pCtx, aMissing, 2);
  check("find two partial-batch source misses", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto batch_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_BATCH_IOERR;
  pBtree = db->aDb[0].pBt;
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pBtree);
  pStore = doltliteBtreeChunkStore(pBtree);
  rc = chunkStoreSourcePrefetchMany(pStore, aMissing, 2);
  zErr = chunkStoreSourceTakeError(pStore, 0);
  sqlite3BtreeLeave(pBtree);
  sqlite3_mutex_leave(db->mutex);
  check("partial batch returns chunk-source IOERR",
        rc==SQLITE_IOERR_CHUNK_SOURCE);
  check("partial batch returned one chunk before IOERR",
        pCtx->nGetMany==1 && pCtx->nReturned==1 && pCtx->faultIssued);
  hashHex(pCtx->faultHash, zHash);
  check("partial batch IOERR names failing hash",
        zErr && strstr(zErr, zHash)!=0);
  rc = chunkStoreOpen(&victim, sqlite3_vfs_find(0), zPath,
      SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  check("open partial-batch victim store", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    victimOpen = 1;
    rc = chunkStoreHas(&victim, &aMissing[0], &has);
    check("partial batch persists no earlier output", rc==SQLITE_OK && !has);
  }

batch_done:
  if( victimOpen ) chunkStoreClose(&victim);
  sqlite3_free(zErr);
  pCtx->mode = SOURCE_NORMAL;
  if( db ){
    doltlite_set_chunk_source(db, "main", 0);
    sqlite3_close(db);
  }
}

static void testMovedWriterFallsBack(
  const char *zPath,
  const char *zAway,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  ProllyHash aMissing[2];
  unsigned char *pData = 0;
  struct stat beforeClear;
  struct stat afterClear;
  int nData = 0;
  int haveBeforeStat = 0;
  int moved = 0;
  int restored = 0;
  int rc;
  memset(aMissing, 0, sizeof(aMissing));
  memset(&beforeClear, 0, sizeof(beforeClear));
  memset(&afterClear, 0, sizeof(afterClear));
  removeStore(zAway);
  check("create moved-writer lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &db);
  check("open moved-writer lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto moved_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register moved-writer source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto moved_done;
  rc = findMissingHashes(db, pCtx, aMissing, 2);
  check("find moved-writer source misses", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto moved_done;
  sourceResetCounters(pCtx);
  rc = getDbChunk(db, &aMissing[0], &pData, &nData);
  check("prime writable cache writer before move", rc==SQLITE_OK);
  sqlite3_free(pData);
  pData = 0;
  if( rc!=SQLITE_OK ) goto moved_done;
  sourceResetCounters(pCtx);
  rc = rename(zPath, zAway);
  check("rename active writable cache file", rc==0);
  if( rc!=0 ) goto moved_done;
  moved = 1;
  rc = getDbChunk(db, &aMissing[1], &pData, &nData);
  check("moved cache writer serves sourced read from memory", rc==SQLITE_OK);
  check("moved cache writer invokes source", pCtx->nRequest>0);
  haveBeforeStat = stat(zAway, &beforeClear)==0;
  check("stat moved cache file before source clear", haveBeforeStat);
  rc = doltlite_set_chunk_source(db, "main", 0);
  check("clear source while cache writer remains moved", rc==SQLITE_OK);
  check("stat moved cache file after source clear",
        stat(zAway, &afterClear)==0);
  check("moved writer close does not append through replacement lock",
        haveBeforeStat && beforeClear.st_size==afterClear.st_size);
  rc = rename(zAway, zPath);
  check("restore active writable cache file", rc==0);
  if( rc==0 ){
    moved = 0;
    restored = 1;
  }
  if( !restored ) goto moved_done;
  sqlite3_free(pData);
  pData = 0;
  rc = getDbChunk(db, &aMissing[1], &pData, &nData);
  check("moved-writer output was memory-only",
        (rc & 0xff)==SQLITE_NOTFOUND);

moved_done:
  sqlite3_free(pData);
  if( moved && rename(zAway, zPath)==0 ) moved = 0;
  if( db ) sqlite3_close(db);
  if( moved ) rename(zAway, zPath);
  removeStore(zAway);
  pCtx->mode = SOURCE_NORMAL;
}

static void testNotFound(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  char *zErr = 0;
  char zHash[41];
  int rc;
  check("create NOTFOUND lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open NOTFOUND lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto notfound_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register NOTFOUND source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto notfound_done;
  rc = execSql(db,
      "SELECT count(*) FROM sqlite_master WHERE name='items'");
  check("warm schema before NOTFOUND injection", rc==SQLITE_OK);
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NOTFOUND;
  rc = runToError(db, "SELECT id FROM items ORDER BY id", &zErr);
  hashHex(pCtx->lastHash, zHash);
  check("source NOTFOUND reaches statement",
        (rc & 0xff)==SQLITE_NOTFOUND);
  check("NOTFOUND error names requested hash",
        zErr && strstr(zErr, zHash)!=0);

notfound_done:
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testCorruptNotPersisted(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  ChunkStore victim;
  ProllyHash hash;
  char *zErr = 0;
  int has = 0;
  int rc;
  memset(&victim, 0, sizeof(victim));
  memset(&hash, 0, sizeof(hash));
  check("create corrupt-byte lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &db);
  check("open corrupt-byte lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto corrupt_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register corrupt-byte source", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = execSql(db,
        "SELECT count(*) FROM sqlite_master WHERE name='items'");
    check("warm schema before corrupt-byte injection", rc==SQLITE_OK);
    sourceResetCounters(pCtx);
    pCtx->mode = SOURCE_CORRUPT;
    rc = runToError(db, "SELECT id FROM items ORDER BY id", &zErr);
    check("corrupt source bytes are rejected",
          (rc & 0xff)==SQLITE_CORRUPT);
    check("corrupt source injected a chunk", pCtx->faultIssued);
  }
  sqlite3_close(db);
  db = 0;
  if( pCtx->faultIssued ){
    memcpy(&hash, pCtx->faultHash, sizeof(hash));
    rc = chunkStoreOpen(&victim, sqlite3_vfs_find(0), zPath,
        SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
    check("open corrupt victim chunk store", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      rc = chunkStoreHas(&victim, &hash, &has);
      check("corrupt chunk was not persisted", rc==SQLITE_OK && !has);
      chunkStoreClose(&victim);
    }
  }

corrupt_done:
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testIoerrReusable(
  const char *zPath,
  sqlite3_int64 expectedRows,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  sqlite3_int64 nRows = 0;
  char *zErr = 0;
  char zHash[41];
  int rc;
  check("create IOERR lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open IOERR lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register IOERR source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto ioerr_done;
  rc = sqlite3_prepare_v2(db,
      "SELECT id,payload FROM items ORDER BY id", -1, &pStmt, 0);
  check("prepare IOERR scan", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_IOERR;
  pCtx->failAfter = 40;
  while( (rc=sqlite3_step(pStmt))==SQLITE_ROW ) nRows++;
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  if( rc!=SQLITE_OK ) zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
  check("IOERR occurs after scan returned rows", rc!=SQLITE_OK && nRows>0);
  check("IOERR uses chunk-source subtype", rc==SQLITE_IOERR_CHUNK_SOURCE);
  hashHex(pCtx->faultHash, zHash);
  check("IOERR error names requested hash",
        pCtx->faultIssued && zErr && strstr(zErr, zHash)!=0);
  sqlite3_finalize(pStmt);
  pStmt = 0;
  pCtx->mode = SOURCE_NORMAL;
  {
    sqlite3_int64 n = 0;
    int retryRc = queryInt64(db, "SELECT count(*) FROM items", &n);
    check("connection is reusable after source IOERR",
          retryRc==SQLITE_OK && n==expectedRows);
  }

ioerr_done:
  sqlite3_free(zErr);
  if( pStmt ) sqlite3_finalize(pStmt);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testScalarRefIoerr(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  static const char zMissingHash[] =
      "1111111111111111111111111111111111111111";
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  char *zErr = 0;
  char *zSql = 0;
  char zHash[41];
  int rc;
  check("create scalar IOERR lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open scalar IOERR lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto scalar_ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register scalar IOERR source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto scalar_ioerr_done;
  zSql = sqlite3_mprintf("SELECT dolt_hashof('%s')", zMissingHash);
  rc = zSql ? sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0) : SQLITE_NOMEM;
  check("prepare scalar ref IOERR query", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto scalar_ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_IOERR;
  pCtx->failAfter = 0;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW && rc!=SQLITE_DONE ){
    zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
  }
  check("scalar ref IOERR uses chunk-source subtype",
        rc==SQLITE_IOERR_CHUNK_SOURCE);
  hashHex(pCtx->faultHash, zHash);
  check("scalar ref IOERR names requested hash",
        pCtx->faultIssued && strcmp(zHash, zMissingHash)==0
        && zErr && strstr(zErr, zHash)!=0);

scalar_ioerr_done:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zSql);
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testPrepareIoerr(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  char zHash[41];
  int rc;
  check("create prepare IOERR lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open prepare IOERR lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto prepare_ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register prepare IOERR source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto prepare_ioerr_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_IOERR;
  pCtx->failAfter = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT id,payload FROM items ORDER BY id", -1, &pStmt, 0);
  check("prepare IOERR uses chunk-source subtype",
        rc==SQLITE_IOERR_CHUNK_SOURCE);
  hashHex(pCtx->faultHash, zHash);
  check("prepare IOERR names requested hash",
        pCtx->faultIssued && strstr(sqlite3_errmsg(db), zHash)!=0);

prepare_ioerr_done:
  if( pStmt ) sqlite3_finalize(pStmt);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testIntegrityErrorConsumed(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  ProllyHash missing;
  char *zIntegrity = 0;
  char *zErr = 0;
  int rc;
  memset(&missing, 0, sizeof(missing));
  check("create integrity source-miss store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open integrity source-miss store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto integrity_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register integrity source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto integrity_done;
  rc = findMissingHashes(db, pCtx, &missing, 1);
  check("find integrity source miss", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto integrity_done;
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, missing.data, PROLLY_HASH_SIZE);
  rc = queryResult(db, "PRAGMA integrity_check(1)", &zIntegrity);
  check("integrity check reports source-missing graph as a result",
        rc==SQLITE_OK && pCtx->faultIssued
        && zIntegrity && strstr(zIntegrity, "integrity check failed")!=0);
  rc = runToError(db, "SELECT abs(-9223372036854775808)", &zErr);
  check("handled integrity miss does not poison a later VDBE error",
        rc==SQLITE_ERROR && zErr && strstr(zErr, "integer overflow")!=0);

integrity_done:
  sqlite3_free(zIntegrity);
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  pCtx->mode = SOURCE_NORMAL;
}

static void testCommitAncestorsSourceMiss(
  const char *zPrefix,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  static const char zMissing[] =
      "0102030405060708090a0b0c0d0e0f1011121314";
  sqlite3 *db = 0;
  char zPath[192];
  char *zSql = 0;
  char *zErr = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), "%s_commit_ancestors.db", zPrefix);
  check("create commit-ancestors source-miss store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open commit-ancestors source-miss store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto commit_ancestors_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register commit-ancestors source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto commit_ancestors_done;
  sourceResetCounters(pCtx);
  zSql = sqlite3_mprintf(
      "SELECT count(*) FROM dolt_commit_ancestors "
      "WHERE commit_hash='%s'", zMissing);
  rc = zSql ? runToError(db, zSql, &zErr) : SQLITE_NOMEM;
  check("commit ancestors propagates sourced missing commit",
        rc==SQLITE_NOTFOUND && pCtx->nGet>0);
  check("commit ancestors source miss names hash",
        zErr && strstr(zErr, zMissing)!=0);

commit_ancestors_done:
  sqlite3_free(zSql);
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  removeStore(zPath);
  pCtx->mode = SOURCE_NORMAL;
}

static void testRegistrationRefreshesPeerAdvance(
  const char *zPath,
  sqlite3 *db,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi
){
  sqlite3 *peer = 0;
  sqlite3_int64 n = 0;
  int rc;

  pCtx->mode = SOURCE_NORMAL;
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &peer);
  check("open peer before source registration", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = execSql(peer,
        "INSERT INTO items VALUES(700001,70,'peer-before-register');"
        "SELECT dolt_commit('-A','-m','peer before source registration')");
  }
  check("peer advances before source registration", rc==SQLITE_OK);
  if( peer ) sqlite3_close(peer);
  peer = 0;

  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register source after peer advance", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = queryInt64(db,
        "SELECT count(*) FROM items WHERE id=700001", &n);
  }
  check("registration preserves peer refresh signal",
        rc==SQLITE_OK && n==1);

  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &peer);
  check("open peer before source clear", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = execSql(peer,
        "INSERT INTO items VALUES(700002,70,'peer-before-clear');"
        "SELECT dolt_commit('-A','-m','peer before source clear')");
  }
  check("peer advances before source clear", rc==SQLITE_OK);
  if( peer ) sqlite3_close(peer);

  rc = doltlite_set_chunk_source(db, "main", 0);
  check("clear source after peer advance", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = queryInt64(db,
        "SELECT count(*) FROM items WHERE id=700002", &n);
  }
  check("source clear preserves peer refresh signal",
        rc==SQLITE_OK && n==1);
}

static void testDetachedInitLazyRejected(
  const char *zPath,
  sqlite3 *db,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *detached = 0;
  char *zHead = 0;
  char zOpen[512];
  int rc;

  rc = queryText(db, "SELECT dolt_hashof('HEAD')", &zHead);
  check("resolve detached bootstrap revision", rc==SQLITE_OK && zHead!=0);
  if( rc==SQLITE_OK ){
    snprintf(zOpen, sizeof(zOpen), "%s/%s", zPath, zHead);
    rc = openDb(zOpen, SQLITE_OPEN_READWRITE, &detached);
    check("open detached bootstrap session", rc==SQLITE_OK);
  }
  if( rc==SQLITE_OK ) rc = doltlite_init_lazy(detached, pRefs, nRefs);
  check("lazy refs reject detached session", rc==SQLITE_READONLY);
  if( detached ) sqlite3_close(detached);
  sqlite3_free(zHead);
}

static void testCatalogMissSurface(
  const char *zName,
  const char *zPath,
  const char *zSql,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs,
  const ProllyHash *pHeadCatalog
){
  sqlite3 *db = 0;
  sqlite3_int64 nPayload = 0;
  char *zErr = 0;
  char zHash[PROLLY_HASH_SIZE*2+1];
  char zLabel[160];
  int rc;

  check("create catalog-miss lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open catalog-miss lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto catalog_surface_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register catalog-miss source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto catalog_surface_done;
  rc = queryInt64(db,
      "SELECT length(payload) FROM oversize_payload", &nPayload);
  check("evict warmed catalog from bounded source cache",
        rc==SQLITE_OK && nPayload==OVERSIZE_BYTES);
  if( rc!=SQLITE_OK ) goto catalog_surface_done;
  rc = resetTreeCache(db);
  check("reset catalog-miss tree cache", rc==SQLITE_OK);
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, pHeadCatalog->data, PROLLY_HASH_SIZE);
  rc = runToError(db, zSql, &zErr);
  snprintf(zLabel, sizeof(zLabel), "%s propagates sourced catalog miss", zName);
  check(zLabel, rc==SQLITE_NOTFOUND && pCtx->faultIssued);
  hashHex(pHeadCatalog->data, zHash);
  snprintf(zLabel, sizeof(zLabel), "%s catalog miss names hash", zName);
  check(zLabel, zErr && strstr(zErr, zHash)!=0);
  sqlite3_free(zErr);
  zErr = 0;
  pCtx->mode = SOURCE_NORMAL;
  rc = runToError(db, "SELECT abs(-9223372036854775808)", &zErr);
  snprintf(zLabel, sizeof(zLabel), "%s consumes source error state", zName);
  check(zLabel, rc==SQLITE_ERROR && zErr
        && strstr(zErr, "integer overflow")!=0);

catalog_surface_done:
  sqlite3_free(zErr);
  if( db ) sqlite3_close(db);
  removeStore(zPath);
  pCtx->mode = SOURCE_NORMAL;
}

static void testResetCatalogMiss(
  const char *zPrefix,
  const char *zSource,
  sqlite3 *sourceDb,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const ProllyHash *pHeadCatalog
){
  sqlite3 *db = 0;
  unsigned char *pRefs = 0;
  sqlite3_int64 nPayload = 0;
  char *zErr = 0;
  char zHash[PROLLY_HASH_SIZE*2+1];
  char zPath[192];
  int nRefs = 0;
  int rc;

  snprintf(zPath, sizeof(zPath), "%s_catalog_reset.db", zPrefix);
  rc = execSql(sourceDb,
      "SELECT dolt_branch('meta'); DROP TABLE meta");
  check("create same-name reset ref with table absent from working", rc==SQLITE_OK);
  sourceClose(pCtx);
  if( rc==SQLITE_OK ) rc = sourceOpen(pCtx, zSource);
  check("refresh source for reset catalog miss", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = serializeRefs(pCtx, &pRefs, &nRefs);
  check("serialize reset catalog-miss refs",
        rc==SQLITE_OK && pRefs!=0 && nRefs>0);
  if( rc!=SQLITE_OK ) goto reset_miss_done;
  check("create reset catalog-miss lazy store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READONLY, &db);
  check("open reset catalog-miss lazy store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto reset_miss_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("register reset catalog-miss source", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto reset_miss_done;
  rc = queryInt64(db,
      "SELECT length(payload) FROM oversize_payload", &nPayload);
  check("evict warmed reset catalog from bounded source cache",
        rc==SQLITE_OK && nPayload==OVERSIZE_BYTES);
  if( rc!=SQLITE_OK ) goto reset_miss_done;
  rc = resetTreeCache(db);
  check("reset catalog-miss reset tree cache", rc==SQLITE_OK);
  sourceResetCounters(pCtx);
  pCtx->mode = SOURCE_ONE_NOTFOUND;
  memcpy(pCtx->faultHash, pHeadCatalog->data, PROLLY_HASH_SIZE);
  rc = runToError(db, "SELECT dolt_reset('meta')", &zErr);
  check("dolt_reset disambiguation propagates sourced catalog miss",
        rc==SQLITE_NOTFOUND && pCtx->faultIssued);
  hashHex(pHeadCatalog->data, zHash);
  check("dolt_reset catalog miss names hash",
        zErr && strstr(zErr, zHash)!=0);
  sqlite3_free(zErr);
  zErr = 0;
  pCtx->mode = SOURCE_NORMAL;
  rc = runToError(db, "SELECT abs(-9223372036854775808)", &zErr);
  check("dolt_reset consumes source error state",
        rc==SQLITE_ERROR && zErr
        && strstr(zErr, "integer overflow")!=0);

reset_miss_done:
  sqlite3_free(zErr);
  sqlite3_free(pRefs);
  if( db ) sqlite3_close(db);
  removeStore(zPath);
  pCtx->mode = SOURCE_NORMAL;
}

static void testCatalogMissSurfaces(
  const char *zPrefix,
  const char *zSource,
  sqlite3 *sourceDb,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi
){
  unsigned char *pRefs = 0;
  ProllyHash head;
  ProllyHash headCatalog;
  char zDiff[192];
  char zDiffRange[192];
  char zWorkspace[192];
  char zDbpage[192];
  int nRefs = 0;
  int rc;

  memset(&head, 0, sizeof(head));
  memset(&headCatalog, 0, sizeof(headCatalog));
  rc = execSql(sourceDb,
      "INSERT INTO items VALUES(800001,80,'dirty-working-catalog')");
  check("create distinct working catalog for miss surfaces", rc==SQLITE_OK);
  doltliteGetSessionHead(sourceDb, &head);
  if( rc==SQLITE_OK ){
    rc = doltliteCommitCatalogHash(sourceDb, &head, &headCatalog);
  }
  check("resolve head catalog for miss surfaces",
        rc==SQLITE_OK && !prollyHashIsEmpty(&headCatalog));
  sourceClose(pCtx);
  if( rc==SQLITE_OK ) rc = sourceOpen(pCtx, zSource);
  check("refresh source after dirty working catalog", rc==SQLITE_OK);
  if( rc==SQLITE_OK ) rc = serializeRefs(pCtx, &pRefs, &nRefs);
  check("serialize dirty working refs",
        rc==SQLITE_OK && pRefs!=0 && nRefs>0);
  if( rc!=SQLITE_OK ) goto catalog_miss_done;

  snprintf(zDiff, sizeof(zDiff), "%s_catalog_diff.db", zPrefix);
  snprintf(zDiffRange, sizeof(zDiffRange),
           "%s_catalog_diff_range.db", zPrefix);
  snprintf(zWorkspace, sizeof(zWorkspace), "%s_catalog_workspace.db", zPrefix);
  snprintf(zDbpage, sizeof(zDbpage), "%s_catalog_dbpage.db", zPrefix);
  testCatalogMissSurface("dolt_diff_items", zDiff,
      "SELECT count(*) FROM dolt_diff_items", pCtx, pApi,
      pRefs, nRefs, &headCatalog);
  testCatalogMissSurface("dolt_diff_items range", zDiffRange,
      "SELECT count(*) FROM dolt_diff_items('HEAD..HEAD')", pCtx, pApi,
      pRefs, nRefs, &headCatalog);
  testCatalogMissSurface("dolt_workspace_items", zWorkspace,
      "SELECT count(*) FROM dolt_workspace_items", pCtx, pApi,
      pRefs, nRefs, &headCatalog);
  testCatalogMissSurface("sqlite_dbpage", zDbpage,
      "SELECT length(data) FROM sqlite_dbpage WHERE pgno=1", pCtx, pApi,
      pRefs, nRefs, &headCatalog);
  testResetCatalogMiss(
      zPrefix, zSource, sourceDb, pCtx, pApi, &headCatalog);

catalog_miss_done:
  sqlite3_free(pRefs);
  pCtx->mode = SOURCE_NORMAL;
}

static void testDeferredAttach(
  const char *zMain,
  const char *zAux,
  sqlite3_int64 expectedRows,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3_int64 nRow = 0;
  char *zSql = 0;
  Btree *pAux;
  int rc;
  check("create refs-only attached store",
        createLazyFile(zAux, pRefs, nRefs)==SQLITE_OK);
  removeStore(zMain);
  rc = openDb(zMain, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  check("open refs-only attach host", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto attach_done;
  zSql = sqlite3_mprintf("ATTACH %Q AS aux", zAux);
  rc = zSql ? execSql(db, zSql) : SQLITE_NOMEM;
  sqlite3_free(zSql);
  zSql = 0;
  check("refs-only ATTACH finishes before source registration", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto attach_done;
  rc = queryInt64(db, "SELECT count(*) FROM aux.items", &nRow);
  check("refs-only attached access without source returns NOTFOUND",
        (rc & 0xff)==SQLITE_NOTFOUND);
  pAux = sqlite3DbNameToBtree(db, "aux");
  if( pAux ){
    char *zMissing = sqlite3_mprintf("main");
    if( zMissing ){
      sqlite3_free(pAux->zBranch);
      pAux->zBranch = zMissing;
    }
  }
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "aux", pApi);
  check("register source for refs-only attached database", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto attach_done;
  check("attached hydration adopts refs default branch",
        pAux && pAux->zBranch && strcmp(pAux->zBranch, "trunk")==0);
  rc = queryInt64(db, "SELECT count(*) FROM aux.items", &nRow);
  check("hydrated attached schema and rows are queryable",
        rc==SQLITE_OK && nRow==expectedRows);

attach_done:
  if( db ) sqlite3_close(db);
  removeStore(zMain);
  removeStore(zAux);
}

static void testDeferredRetry(
  const char *zPath,
  sqlite3_int64 expectedRows,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3_int64 nRow = 0;
  Btree *pMain;
  int rc;
  removeStore(zPath);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  check("open transient-hydration retry host", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto retry_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("install source before lazy refs refresh", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto retry_done;
  pCtx->mode = SOURCE_NOTFOUND;
  rc = doltlite_init_lazy(db, pRefs, nRefs);
  check("lazy refs hydration miss remains retryable",
        (rc & 0xff)==SQLITE_NOTFOUND);
  pMain = sqlite3DbNameToBtree(db, "main");
  check("failed lazy refs hydration remains deferred",
        pMain && pMain->bDeferredOpen);
  if( pMain ){
    char *zMissing = sqlite3_mprintf("main");
    if( zMissing ){
      sqlite3_free(pMain->zBranch);
      pMain->zBranch = zMissing;
    }
  }
  pCtx->mode = SOURCE_NORMAL;
  rc = queryInt64(db, "SELECT count(*) FROM items", &nRow);
  check("prepare retries hydration and reloads schema",
        rc==SQLITE_OK && nRow==expectedRows);
  check("deferred module registration completes after retry",
        pMain && !pMain->bDeferredOpen && !pMain->bDeferredRegister);
  rc = queryInt64(db, "SELECT count(*) FROM dolt_at_items('HEAD')", &nRow);
  check("retry registers catalog-dependent historical module",
        rc==SQLITE_OK && nRow==expectedRows);
  check("retry hydration adopts refs default branch",
        pMain && pMain->zBranch && strcmp(pMain->zBranch, "trunk")==0);

retry_done:
  pCtx->mode = SOURCE_NORMAL;
  if( db ) sqlite3_close(db);
  removeStore(zPath);
}

static void testDeferredSchemaCookie(
  const char *zPath,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  Btree *pMain;
  ChunkStore *pStore;
  int schemaVersion = -1;
  int rc;
  check("create schema-cookie refs-only store",
        createLazyFile(zPath, pRefs, nRefs)==SQLITE_OK);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE, &db);
  check("open schema-cookie refs-only store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto cookie_done;
  pMain = sqlite3DbNameToBtree(db, "main");
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnter(pMain);
  pStore = doltliteBtreeChunkStore(pMain);
  pCtx->mode = SOURCE_NORMAL;
  rc = chunkStoreSourceSet(pStore, db, pApi, 0);
  if( rc==SQLITE_OK ) rc = prollyBtreeBeginTrans(pMain, 1, &schemaVersion);
  check("write begin hydrates refs-only store", rc==SQLITE_OK);
  check("write begin returns post-hydration schema cookie",
        rc==SQLITE_OK
        && schemaVersion==(int)pMain->aMeta[BTREE_SCHEMA_VERSION]
        && schemaVersion!=0);
  if( rc==SQLITE_OK ) prollyBtreeRollback(pMain, SQLITE_OK, 0);
  chunkStoreSourceClose(pStore);
  sqlite3BtreeLeave(pMain);
  sqlite3_mutex_leave(db->mutex);

cookie_done:
  if( db ) sqlite3_close(db);
  removeStore(zPath);
}

static void testDeferredRefsCommitFailure(
  const char *zPath,
  sqlite3_int64 expectedRows,
  SourceCtx *pCtx,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  sqlite3_int64 nRow = 0;
  Btree *pMain;
  int rc;
  removeStore(zPath);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  check("open post-commit fallback-failure store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto refs_failure_done;
  pCtx->mode = SOURCE_NORMAL;
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("install source before fallback allocation failure", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto refs_failure_done;
  faultCode = 962;
  sqlite3_test_control(
      SQLITE_TESTCTRL_FAULT_INSTALL, chunkSourceFaultCallback);
  rc = doltlite_init_lazy(db, pRefs, nRefs);
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  faultCode = 0;
  check("fallback allocation fails after refs commit", rc==SQLITE_NOMEM);
  pMain = sqlite3DbNameToBtree(db, "main");
  check("post-commit allocation failure leaves Btree deferred",
        pMain && pMain->bDeferredOpen);
  rc = queryInt64(db, "SELECT count(*) FROM items", &nRow);
  check("post-commit allocation failure retries new refs",
        rc==SQLITE_OK && nRow==expectedRows);

refs_failure_done:
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_INSTALL, 0);
  faultCode = 0;
  if( db ) sqlite3_close(db);
  removeStore(zPath);
}

static void testBareBeginApiGuards(
  const char *zPath,
  doltlite_chunk_source *pApi,
  const unsigned char *pRefs,
  int nRefs
){
  sqlite3 *db = 0;
  int rc;
  removeStore(zPath);
  rc = openDb(zPath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &db);
  check("open bare-BEGIN API guard store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto guard_done;
  rc = execSql(db, "BEGIN");
  check("begin transaction before source registration", rc==SQLITE_OK);
  rc = doltlite_set_chunk_source(db, "main", pApi);
  check("bare BEGIN blocks chunk source registration", rc==SQLITE_BUSY);
  rc = execSql(db, "ROLLBACK");
  check("rollback source-registration guard transaction", rc==SQLITE_OK);
  rc = execSql(db, "BEGIN");
  check("begin transaction before lazy refs install", rc==SQLITE_OK);
  rc = doltlite_init_lazy(db, pRefs, nRefs);
  check("bare BEGIN blocks lazy refs install", rc==SQLITE_BUSY);
  rc = execSql(db, "ROLLBACK");
  check("rollback lazy-refs guard transaction", rc==SQLITE_OK);

guard_done:
  if( db ) sqlite3_close(db);
  removeStore(zPath);
}

static void testDeferredHydrationInfrastructure(const char *zPrefix){
  sqlite3 *sourceDb = 0;
  SourceCtx source;
  doltlite_chunk_source api;
  unsigned char *pRefs = 0;
  int nRefs = 0;
  sqlite3_int64 expectedRows = 0;
  char zSource[192];
  char zAttachMain[192];
  char zAttachAux[192];
  char zRetry[192];
  char zCookie[192];
  char zRefsFailure[192];
  char zGuard[192];
  int rc;
  memset(&source, 0, sizeof(source));
  snprintf(zSource, sizeof(zSource), "%s_deferred_source.db", zPrefix);
  snprintf(zAttachMain, sizeof(zAttachMain), "%s_attach_main.db", zPrefix);
  snprintf(zAttachAux, sizeof(zAttachAux), "%s_attach_aux.db", zPrefix);
  snprintf(zRetry, sizeof(zRetry), "%s_deferred_retry.db", zPrefix);
  snprintf(zCookie, sizeof(zCookie), "%s_schema_cookie.db", zPrefix);
  snprintf(zRefsFailure, sizeof(zRefsFailure),
           "%s_refs_commit_failure.db", zPrefix);
  snprintf(zGuard, sizeof(zGuard), "%s_api_guard.db", zPrefix);
  rc = buildSource(zSource, &sourceDb);
  check("build renamed-default source repository", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto deferred_done;
  rc = execSql(sourceDb, "SELECT dolt_branch('-m','main','trunk')");
  check("rename source default branch", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto deferred_done;
  rc = queryInt64(sourceDb, "SELECT count(*) FROM items", &expectedRows);
  if( rc==SQLITE_OK ) rc = sourceOpen(&source, zSource);
  check("open renamed-default source store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto deferred_done;
  initSourceApi(&source, &api);
  rc = serializeRefs(&source, &pRefs, &nRefs);
  check("serialize renamed-default refs", rc==SQLITE_OK && pRefs && nRefs>0);
  if( rc!=SQLITE_OK ) goto deferred_done;
  testDeferredAttach(zAttachMain, zAttachAux, expectedRows,
      &source, &api, pRefs, nRefs);
  testDeferredRetry(zRetry, expectedRows, &source, &api, pRefs, nRefs);
  testDeferredSchemaCookie(zCookie, &source, &api, pRefs, nRefs);
  testDeferredRefsCommitFailure(
      zRefsFailure, expectedRows, &source, &api, pRefs, nRefs);
  testBareBeginApiGuards(zGuard, &api, pRefs, nRefs);

deferred_done:
  sourceClose(&source);
  if( sourceDb ) sqlite3_close(sourceDb);
  sqlite3_free(pRefs);
  removeStore(zSource);
  removeStore(zAttachMain);
  removeStore(zAttachAux);
  removeStore(zRetry);
  removeStore(zCookie);
  removeStore(zRefsFailure);
  removeStore(zGuard);
}

int main(void){
  sqlite3 *sourceDb = 0;
  SourceCtx source;
  doltlite_chunk_source api;
  unsigned char *pRefs = 0;
  unsigned char *pNewRefs = 0;
  int nRefs = 0;
  int nNewRefs = 0;
  sqlite3_int64 expectedRows = 0;
  char zPrefix[160];
  char zSource[192];
  char zWrite[192];
  char zReadOnly[192];
  char zDetach[192];
  char zWorkingSetMiss[192];
  char zOtherWorkingSetMiss[192];
  char zGraphLock[192];
  char zGraphAux[192];
  char zBatchIoerr[192];
  char zMoved[192];
  char zMovedAway[208];
  char zNotFound[192];
  char zCorrupt[192];
  char zIoerr[192];
  char zScalarIoerr[192];
  char zPrepareIoerr[192];
  char zIntegrity[192];
  int rc;

  sqlite3_initialize();
  memset(&source, 0, sizeof(source));
  snprintf(zPrefix, sizeof(zPrefix),
           "/tmp/doltlite_chunk_source_%ld", (long)getpid());
  snprintf(zSource, sizeof(zSource), "%s_source.db", zPrefix);
  snprintf(zWrite, sizeof(zWrite), "%s_write.db", zPrefix);
  snprintf(zReadOnly, sizeof(zReadOnly), "%s_readonly.db", zPrefix);
  snprintf(zDetach, sizeof(zDetach), "%s_detach.db", zPrefix);
  snprintf(zWorkingSetMiss, sizeof(zWorkingSetMiss),
           "%s_working_set_miss.db", zPrefix);
  snprintf(zOtherWorkingSetMiss, sizeof(zOtherWorkingSetMiss),
           "%s_other_working_set_miss.db", zPrefix);
  snprintf(zGraphLock, sizeof(zGraphLock), "%s_graph_lock.db", zPrefix);
  snprintf(zGraphAux, sizeof(zGraphAux), "%s_graph_aux.db", zPrefix);
  snprintf(zBatchIoerr, sizeof(zBatchIoerr), "%s_batch_ioerr.db", zPrefix);
  snprintf(zMoved, sizeof(zMoved), "%s_moved.db", zPrefix);
  snprintf(zMovedAway, sizeof(zMovedAway), "%s_moved.db.away", zPrefix);
  snprintf(zNotFound, sizeof(zNotFound), "%s_notfound.db", zPrefix);
  snprintf(zCorrupt, sizeof(zCorrupt), "%s_corrupt.db", zPrefix);
  snprintf(zIoerr, sizeof(zIoerr), "%s_ioerr.db", zPrefix);
  snprintf(zScalarIoerr, sizeof(zScalarIoerr),
           "%s_scalar_ioerr.db", zPrefix);
  snprintf(zPrepareIoerr, sizeof(zPrepareIoerr),
           "%s_prepare_ioerr.db", zPrefix);
  snprintf(zIntegrity, sizeof(zIntegrity), "%s_integrity.db", zPrefix);

  printf("DoltLite host chunk source test\n");
  printf("===============================\n");

  rc = buildSource(zSource, &sourceDb);
  check("build fully populated source repository", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto test_done;
  testNoSourceAncestorError(sourceDb);
  rc = sourceOpen(&source, zSource);
  check("open source ChunkStore", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto test_done;
  initSourceApi(&source, &api);
  rc = serializeRefs(&source, &pRefs, &nRefs);
  check("serialize initial refs", rc==SQLITE_OK && pRefs && nRefs>0);
  if( rc!=SQLITE_OK ) goto test_done;

  testOracleWriteThroughAndRefresh(
      zSource, zWrite, sourceDb, &source, &api, pRefs, nRefs,
      &pNewRefs, &nNewRefs);
  if( !pNewRefs ) goto test_done;
  rc = queryInt64(sourceDb, "SELECT count(*) FROM items", &expectedRows);
  check("count advanced source rows", rc==SQLITE_OK && expectedRows>BASE_ROWS);

  sourceResetCounters(&source);
  source.mode = SOURCE_NORMAL;
  testReadOnlyCacheAndBatching(
      zReadOnly, sourceDb, &source, &api, pNewRefs, nNewRefs);
  testFailedRegistrationDetaches(
      zDetach, &source, &api, pNewRefs, nNewRefs);
  testWorkingSetNotFound(
      zWorkingSetMiss, &source, &api, pNewRefs, nNewRefs);
  testOtherBranchWorkingSetNotFound(
      zOtherWorkingSetMiss, &source, &api, pNewRefs, nNewRefs);
  testGraphLockMiss(
      zGraphLock, zGraphAux, &source, &api, pNewRefs, nNewRefs);
  testBatchPartialIoerr(
      zBatchIoerr, &source, &api, pNewRefs, nNewRefs);
  testMovedWriterFallsBack(
      zMoved, zMovedAway, &source, &api, pNewRefs, nNewRefs);
  testNotFound(zNotFound, &source, &api, pNewRefs, nNewRefs);
  testCorruptNotPersisted(zCorrupt, &source, &api, pNewRefs, nNewRefs);
  testIoerrReusable(
      zIoerr, expectedRows, &source, &api, pNewRefs, nNewRefs);
  testScalarRefIoerr(
      zScalarIoerr, &source, &api, pNewRefs, nNewRefs);
  testPrepareIoerr(
      zPrepareIoerr, &source, &api, pNewRefs, nNewRefs);
  testCommitAncestorsSourceMiss(
      zPrefix, &source, &api, pNewRefs, nNewRefs);
  testIntegrityErrorConsumed(
      zIntegrity, &source, &api, pNewRefs, nNewRefs);
  testDeferredHydrationInfrastructure(zPrefix);
  testDetachedInitLazyRejected(zSource, sourceDb, pNewRefs, nNewRefs);
  testRegistrationRefreshesPeerAdvance(zSource, sourceDb, &source, &api);
  testCatalogMissSurfaces(zPrefix, zSource, sourceDb, &source, &api);

test_done:
  sourceClose(&source);
  if( sourceDb ) sqlite3_close(sourceDb);
  sqlite3_free(pRefs);
  sqlite3_free(pNewRefs);
  removeStore(zSource);
  removeStore(zWrite);
  removeStore(zReadOnly);
  removeStore(zDetach);
  removeStore(zWorkingSetMiss);
  removeStore(zOtherWorkingSetMiss);
  removeStore(zGraphLock);
  removeStore(zGraphAux);
  removeStore(zBatchIoerr);
  removeStore(zMoved);
  removeStore(zMovedAway);
  removeStore(zNotFound);
  removeStore(zCorrupt);
  removeStore(zIoerr);
  removeStore(zScalarIoerr);
  removeStore(zPrepareIoerr);
  removeStore(zIntegrity);
  sqlite3_shutdown();
  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}

#else
int main(void){ return 0; }
#endif
