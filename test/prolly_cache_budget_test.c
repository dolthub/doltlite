#include "prolly_btree_int.h"
#include "chunk_store_int.h"
#include <stdio.h>
#include <unistd.h>

static int nPass;
static int nFail;

static void check(const char *zName, int ok){
  if( ok ){
    nPass++;
  }else{
    fprintf(stderr, "FAIL: %s\n", zName);
    nFail++;
  }
}

static void execSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ) fprintf(stderr, "%s: %s\n", zSql, zErr);
  check(zSql, rc==SQLITE_OK);
  sqlite3_free(zErr);
}

static i64 indexCacheBytes(const ChunkStore *cs){
  return cs->pIndexCache ? cs->pIndexCache->nByte : 0;
}

static sqlite3_int64 cacheBytes(ProllyCache *pCache){
  ProllyCacheEntry *p;
  sqlite3_int64 n = sqlite3_msize(pCache->aBucket);
  for(p=pCache->lruHead.pLruNext; p!=&pCache->lruTail; p=p->pLruNext){
    n += sqlite3_msize(p) + sqlite3_msize(p->pData);
  }
  for(p=pCache->prefixHead.pLruNext; p!=&pCache->prefixTail; p=p->pLruNext){
    n += sqlite3_msize(p) + sqlite3_msize(p->pData);
  }
  return n;
}

static sqlite3_int64 combinedCacheBytes(sqlite3 *db){
  return cacheBytes(doltliteGetCache(db))
       + indexCacheBytes(doltliteGetChunkStore(db));
}

static int budgetMatches(sqlite3 *db, sqlite3_int64 nByte){
  ProllyCache *pCache = doltliteGetCache(db);
  return pCache->nMaxByte<=nByte && pCache->nMaxByte>=nByte-nByte/16
      && nByte-pCache->nMaxByte<=4*1024*1024
      && combinedCacheBytes(db)<=nByte;
}

static void testReload(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int nSlot = cs->nIndexCacheSlot;
  int rc = chunkStoreLockAndRefresh(cs);
  check("lock for cache reload", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  rc = csReloadFromDiskPreservingLocalRefs(cs);
  chunkStoreUnlock(cs);
  check("reload cached store", rc==SQLITE_OK);
  check("reload preserves configured index cache slots", cs->nIndexCacheSlot==nSlot);
  check("reload discards differently sized index cache", indexCacheBytes(cs)==0);
}

static void scan(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare scan", sqlite3_prepare_v2(db,
      "SELECT count(v), sum(length(v)), sum(id),"
      " sum(v=printf('%0100d',id)) FROM t", -1, &p, 0)
      ==SQLITE_OK);
  check("scan result", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==100000
      && sqlite3_column_int(p, 1)==10000000
      && sqlite3_column_int64(p, 2)==5000050000LL
      && sqlite3_column_int(p, 3)==100000);
  check("finish scan", sqlite3_finalize(p)==SQLITE_OK);
}

static const sqlite3_io_methods *pReadMethods;
static int nRead;
static int nBatchRead;
static int eReadFault;
static i64 iReadAheadFault = -1;

static int countedRead(sqlite3_file *pFile, void *pData, int n, sqlite3_int64 off){
  int rc;
  nRead++;
  if( n>16384 ){
    nBatchRead++;
    if( eReadFault==1 ) return SQLITE_IOERR_READ;
  }
  rc = pReadMethods->xRead(pFile, pData, n, off);
  if( rc==SQLITE_OK && iReadAheadFault>=off && iReadAheadFault-off<n ){
    ((u8*)pData)[iReadAheadFault-off] ^= 1;
  }
  if( rc==SQLITE_OK && n>16384 ){
    if( eReadFault==2 ) ((u8*)pData)[0] ^= 1;
    if( eReadFault==3 ) ((u8*)pData)[n-1] ^= 1;
  }
  return rc;
}

static void clearNodes(ProllyCache *pCache){
  sqlite3_int64 nBudget = pCache->nMaxByte;
  prollyCacheSetBudget(pCache, 4096);
  prollyCacheSetBudget(pCache, nBudget);
  nRead = nBatchRead = 0;
}

static void testReadAhead(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  ProllyCacheEntry *pEntry;
  sqlite3_io_methods methods;
  sqlite3_stmt *p = 0;
  char zSql[200];
  i64 boundary = 0;
  int nLeaf = 0;
  int i;

  for(pEntry=pCache->lruHead.pLruNext; pEntry!=&pCache->lruTail;
      pEntry=pEntry->pLruNext){
    ProllyNode *pNode = &pEntry->node;
    if( pNode->level==0 && pNode->nItems>0
     && (pNode->flags & PROLLY_NODE_INTKEY)!=0 ){
      i64 key = prollyNodeIntKey(pNode, pNode->nItems-1);
      nLeaf++;
      if( key>20000 && key<90000 ) boundary = key;
    }
  }
  check("read-ahead fixture spans many leaves", nLeaf>100 && boundary>0);
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  scan(db);
  check("scan batches adjacent leaf reads", nBatchRead>0 && nRead<nLeaf/2);
  nRead = nBatchRead = 0;
  scan(db);
  check("cached scan needs no read-ahead", nBatchRead==0);

  clearNodes(pCache);
  check("prepare point read", sqlite3_prepare_v2(db,
      "SELECT length(v) FROM t WHERE id=50000", -1, &p, 0)==SQLITE_OK);
  check("point read result", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==100);
  check("finish point read", sqlite3_finalize(p)==SQLITE_OK);
  check("point read avoids read-ahead", nBatchRead==0);

  clearNodes(pCache);
  sqlite3_snprintf(sizeof(zSql), zSql,
      "SELECT count(v), sum(length(v)) FROM t WHERE id BETWEEN %lld AND %lld",
      boundary, boundary+1);
  check("prepare short range", sqlite3_prepare_v2(db, zSql, -1, &p, 0)
      ==SQLITE_OK);
  check("short range crosses leaf boundary", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==2 && sqlite3_column_int(p, 1)==200);
  check("finish short range", sqlite3_finalize(p)==SQLITE_OK);
  check("short range avoids read-ahead", nBatchRead==0);

  for(i=1; i<=3; i++){
    clearNodes(pCache);
    eReadFault = i;
    scan(db);
    check("speculative read failure falls back", nBatchRead>0);
    check("read-ahead obeys cache budget", pCache->nByte<=pCache->nMaxByte);
  }
  eReadFault = 0;
  execSql(db, "PRAGMA cache_size=-64");
  nRead = nBatchRead = 0;
  scan(db);
  check("small cache avoids read-ahead", nBatchRead==0);
  check("small scan stays within budget", pCache->nByte<=pCache->nMaxByte);
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "PRAGMA cache_size=-65536");
}

typedef struct ReadAheadCheck ReadAheadCheck;
struct ReadAheadCheck {
  ProllyCache *pCache;
  ProllyHash *aHash;
  int nCached;
  int nRead;
  int seen;
  int bEvict;
};

static int readAheadCached(void *pCtx, const ProllyHash *pHash){
  ReadAheadCheck *p = pCtx;
  ProllyCacheEntry *pEntry = prollyCacheGetForScan(p->pCache, pHash);
  if( !pEntry ) return 0;
  p->nCached++;
  prollyCacheRelease(p->pCache, pEntry);
  return 1;
}

static int readAheadReceived(
  void *pCtx, const ProllyHash *pHash, const u8 *pData, int nData
){
  ReadAheadCheck *p = pCtx;
  ProllyHash actual;
  int i;
  prollyHashCompute(pData, nData, &actual);
  check("read-ahead delivers verified bytes", prollyHashCompare(&actual, pHash)==0);
  p->nRead++;
  for(i=0; i<4; i++){
    if( prollyHashCompare(pHash, &p->aHash[i])==0 ) p->seen |= 1<<i;
  }
  if( p->bEvict ) prollyCacheShrink(p->pCache);
  return SQLITE_OK;
}

static void testCachedReadAhead(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  ProllyCacheEntry *pEntry;
  ChunkIndexEntry aEntry[4];
  ProllyHash aHash[4];
  sqlite3_io_methods methods;
  ReadAheadCheck ctx;
  int nHash = 0;
  int rc;
  int i;

  scan(db);
  for(pEntry=pCache->lruHead.pLruNext; pEntry!=&pCache->lruTail;
      pEntry=pEntry->pLruNext){
    if( pEntry->node.level!=1 || pEntry->node.nItems<4 ) continue;
    for(i=0; i<4; i++){
      int found = 0;
      prollyNodeChildHash(&pEntry->node, i, &aHash[i]);
      rc = csIndexLookup(pStore, &aHash[i], &aEntry[i], &found);
      if( rc!=SQLITE_OK || !found ) break;
      if( i>0 && (aEntry[i].offset<aEntry[i-1].offset+aEntry[i-1].size+4
       || aEntry[i].offset>aEntry[i-1].offset+aEntry[i-1].size+4
                                  +CS_WAL_CHUNK_HDR_SIZE) ) break;
      if( aEntry[i].offset+aEntry[i].size+4-aEntry[0].offset
          >CHUNK_READ_AHEAD_BYTES ) break;
    }
    if( i==4 ){ nHash = 4; break; }
  }
  check("find adjacent read-ahead leaves", nHash==4);
  if( nHash!=4 ) return;
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;

  prollyCacheShrink(pCache);
  rc = prollyLoadNode(pStore, pCache, &aHash[1], &pEntry);
  check("cache one read-ahead leaf", rc==SQLITE_OK);
  if( pEntry ) prollyCacheRelease(pCache, pEntry);
  for(i=0; i<3; i++){
    memset(&ctx, 0, sizeof(ctx));
    ctx.pCache = pCache;
    ctx.aHash = aHash;
    iReadAheadFault = i==0 ? -1 : aEntry[1].offset+(i==2 ? 4 : 0);
    rc = chunkStoreReadAhead(pStore, aHash, nHash,
                            readAheadCached, readAheadReceived, &ctx);
    check("cached leaf is not consumed from read-ahead", rc==SQLITE_OK
        && ctx.nCached==1 && ctx.nRead==3 && ctx.seen==13);
  }
  for(i=0; i<2; i++){
    memset(&ctx, 0, sizeof(ctx));
    ctx.pCache = pCache;
    ctx.aHash = aHash;
    iReadAheadFault = aEntry[2].offset+(i ? 4 : 0);
    rc = chunkStoreReadAhead(pStore, aHash, nHash,
                            readAheadCached, readAheadReceived, &ctx);
    check("uncached read-ahead corruption never reaches callback",
          rc==SQLITE_CORRUPT && ctx.nCached==1 && ctx.nRead==1
          && ctx.seen==1);
  }
  iReadAheadFault = -1;
  memset(&ctx, 0, sizeof(ctx));
  ctx.pCache = pCache;
  ctx.aHash = aHash;
  ctx.bEvict = 1;
  rc = chunkStoreReadAhead(pStore, aHash, nHash,
                          readAheadCached, readAheadReceived, &ctx);
  check("recheck cached leaf after earlier callback evicts it", rc==SQLITE_OK
      && ctx.nCached==0 && ctx.nRead==4 && ctx.seen==15);
  check("read-ahead cache accounting", pCache->nByte==cacheBytes(pCache)
      && pCache->nByte<=pCache->nMaxByte);
  pStore->file.pFile->pMethods = pReadMethods;
}

static void pointReads(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare hot point reads", sqlite3_prepare_v2(db,
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c"
      " WHERE i<200) SELECT sum(v=printf('%0100d',id)) FROM c JOIN t"
      " ON t.id=1+(i*313)%100000", -1, &p, 0)==SQLITE_OK);
  check("hot point results", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==200);
  check("finish hot point reads", sqlite3_finalize(p)==SQLITE_OK);
}

static void smallScan(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare small scan", sqlite3_prepare_v2(db,
      "SELECT count(*),sum(v=printf('%0100d',id)) FROM small", -1,
      &p, 0)==SQLITE_OK);
  check("small scan results", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==5000
      && sqlite3_column_int(p, 1)==5000);
  check("finish small scan", sqlite3_finalize(p)==SQLITE_OK);
}

static void rangeScan(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare bounded range", sqlite3_prepare_v2(db,
      "SELECT count(*),sum(v=printf('%0100d',id)) FROM t"
      " WHERE id BETWEEN 90001 AND 95000", -1, &p, 0)==SQLITE_OK);
  check("bounded range results", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==5000
      && sqlite3_column_int(p, 1)==5000);
  check("finish bounded range", sqlite3_finalize(p)==SQLITE_OK);
}

static void testLargeScans(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  int nCold;
  int nPoint;

  execSql(db, "CREATE TABLE small(id INTEGER PRIMARY KEY, v TEXT);"
      "INSERT INTO small SELECT * FROM t WHERE id<=5000");
  execSql(db, "PRAGMA cache_size=-2000");
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;

  clearNodes(pCache);
  scan(db);
  clearNodes(pCache);
  scan(db);
  nCold = nRead;
  nRead = 0;
  scan(db);
  check("large scan reuses part of previous pass", nCold>0
      && nRead<nCold*95/100);
  check("large scan accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 2000*1024));

  clearNodes(pCache);
  pointReads(db);
  nPoint = nRead;
  pointReads(db);
  scan(db);
  nRead = 0;
  pointReads(db);
  check("large scan preserves hot point leaves", nPoint>0
      && nRead<nPoint/4);
  smallScan(db);
  nRead = 0;
  smallScan(db);
  check("small table still fills cache after large scan", nRead==0);
  scan(db);
  nRead = 0;
  smallScan(db);
  check("large scan preserves small table", nRead==0);
  rangeScan(db);
  nRead = 0;
  rangeScan(db);
  check("bounded range still fills cache", nRead==0);
  check("mixed scan accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 2000*1024));
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "PRAGMA cache_size=-65536");
}

static void testNearBudgetScans(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  int nCold;

  execSql(db, "PRAGMA cache_size=-8192");
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  scan(db);
  nCold = nRead;
  nRead = 0;
  scan(db);
  check("scan just beyond cache budget reuses previous pass", nCold>0
      && nRead<nCold*3/4);
  check("near-budget scan accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 8192*1024));
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "PRAGMA cache_size=-65536");
}

static void wideScalarScan(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare wide scalar scan", sqlite3_prepare_v2(db,
      "SELECT sum(v),sum(length(b)) FROM wide NOT INDEXED", -1, &p, 0)
      ==SQLITE_OK);
  check("wide scalar result", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==32896
      && sqlite3_column_int(p, 1)==4194304);
  check("finish wide scalar scan", sqlite3_finalize(p)==SQLITE_OK);
}

static void testWidePrefixes(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  sqlite3_stmt *p = 0;
  int nCold;
  int i;
  execSql(db, "CREATE TABLE wide(id TEXT PRIMARY KEY,v INTEGER,b BLOB,tail TEXT);"
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<256)"
      " INSERT INTO wide SELECT printf('%08d',i),i,"
      " CAST(printf('%016384d',i) AS BLOB),printf('tail-%d',i) FROM c;"
      " PRAGMA cache_size=-512");
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  execSql(db, "SELECT sum(v) FROM (SELECT v FROM wide ORDER BY id DESC)");
  execSql(db, "SELECT sum(v) FROM (SELECT v FROM wide ORDER BY id DESC)");
  nRead = 0;
  execSql(db, "SELECT sum(v) FROM (SELECT v FROM wide ORDER BY id DESC)");
  check("backward scan retains wide prefixes", nRead==0);
  clearNodes(pCache);
  execSql(db, "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c"
      " WHERE i<512) SELECT sum((SELECT v FROM wide"
      " WHERE id=printf('%08d',1+(i*313)%256))) FROM c");
  nRead = 0;
  execSql(db, "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c"
      " WHERE i<512) SELECT sum((SELECT v FROM wide"
      " WHERE id=printf('%08d',1+(i*313)%256))) FROM c");
  check("point lookups retain wide prefixes", nRead==0);
  clearNodes(pCache);
  wideScalarScan(db);
  nCold = nRead;
  nRead = 0;
  wideScalarScan(db);
  check("wide scalar scan retains prefixes without rereading payloads",
        nCold>0 && nRead==0);
  check("prefix cache accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 512*1024));
  for(i=0; i<3; i++){
    execSql(db, "BEGIN; UPDATE wide SET tail=tail||'-pending' WHERE v%2=0");
    nRead = 0;
    wideScalarScan(db);
    check("updates retain prefixes for the first scalar scan", nRead==0);
    check("updated prefix cache accounting", cacheBytes(pCache)==pCache->nByte
        && budgetMatches(db, 512*1024));
    execSql(db, "ROLLBACK");
  }
  check("prepare pinned prefix scan", sqlite3_prepare_v2(db,
      "SELECT id,v FROM wide ORDER BY id", -1, &p, 0)==SQLITE_OK);
  check("pin prefix row", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 1)==1);
  execSql(db, "SELECT sum(b=CAST(printf('%016384d',v) AS BLOB)) FROM wide");
  execSql(db, "PRAGMA shrink_memory; PRAGMA cache_size=0");
  for(i=2; sqlite3_step(p)==SQLITE_ROW; i++){
    if( sqlite3_column_int(p, 1)!=i ) break;
  }
  check("pinned prefixes survive full reads and cache shrink", i==257);
  check("finish pinned prefix scan", sqlite3_finalize(p)==SQLITE_OK);
  check("prefix cache shrinks to minimum", budgetMatches(db, 4096));
  execSql(db, "PRAGMA cache_size=-512");
  wideScalarScan(db);
  check("prepare full wide values", sqlite3_prepare_v2(db,
      "SELECT sum(b=CAST(printf('%016384d',v) AS BLOB)),"
      " sum(tail=printf('tail-%d',v)) FROM wide", -1, &p, 0)==SQLITE_OK);
  check("reload nonzero payloads and columns beyond prefix",
      sqlite3_step(p)==SQLITE_ROW && sqlite3_column_int(p, 0)==256
      && sqlite3_column_int(p, 1)==256);
  check("finish full wide values", sqlite3_finalize(p)==SQLITE_OK);
  wideScalarScan(db);
  wideScalarScan(db);
  eReadFault = 1;
  check("prepare failing full value", sqlite3_prepare_v2(db,
      "SELECT hex(b) FROM wide WHERE id='00000128'", -1, &p, 0)==SQLITE_OK);
  check("prefix fallback propagates read errors", sqlite3_step(p)==SQLITE_IOERR);
  sqlite3_finalize(p);
  eReadFault = 0;
  eReadFault = 3;
  check("prepare corrupt full value", sqlite3_prepare_v2(db,
      "SELECT hex(b) FROM wide WHERE id='00000128'", -1, &p, 0)==SQLITE_OK);
  check("prefix fallback verifies the entire chunk",
        sqlite3_step(p)==SQLITE_CORRUPT);
  sqlite3_finalize(p);
  eReadFault = 0;
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "DROP TABLE wide; PRAGMA cache_size=-65536");
}

static void testWidePrefixPressure(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  sqlite3_stmt *p = 0;
  int i, nCold;
  execSql(db, "CREATE TABLE wide_pressure(id TEXT PRIMARY KEY,v INTEGER,"
      " pad TEXT,b BLOB,tail TEXT);"
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<4096)"
      " INSERT INTO wide_pressure SELECT printf('%016x',i),i,"
      " printf('%080d',i),CAST(printf('%016384d',i) AS BLOB),"
      " printf('tail-%d',i) FROM c; PRAGMA cache_size=-1024");
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  nCold = 0;
  for(i=0; i<3; i++){
    nRead = 0;
    check("prepare pressure scan", sqlite3_prepare_v2(db,
        "SELECT sum(v),sum(length(b)) FROM wide_pressure", -1, &p, 0)
        ==SQLITE_OK);
    check("pressure scan result", sqlite3_step(p)==SQLITE_ROW
        && sqlite3_column_int(p, 0)==8390656
        && sqlite3_column_int(p, 1)==67108864);
    check("finish pressure scan", sqlite3_finalize(p)==SQLITE_OK);
    if( i==0 ) nCold = nRead;
  }
  check("wide prefixes compact before eviction", nCold>0 && nRead<nCold/20);
  check("compacted prefix accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 1024*1024));
  for(i=0; i<3; i++){
    nRead = 0;
    check("prepare larger prefix scan", sqlite3_prepare_v2(db,
        "SELECT sum(pad=printf('%080d',v)) FROM wide_pressure", -1, &p, 0)
        ==SQLITE_OK);
    check("larger prefix scan result", sqlite3_step(p)==SQLITE_ROW
        && sqlite3_column_int(p, 0)==4096);
    check("finish larger prefix scan", sqlite3_finalize(p)==SQLITE_OK);
  }
  check("prefix fallback preserves reuse for later rows", nRead<nCold*9/10);
  check("prepare compacted prefix fallback", sqlite3_prepare_v2(db,
      "SELECT sum(pad=printf('%080d',v)),"
      " sum(b=CAST(printf('%016384d',v) AS BLOB)),"
      " sum(tail=printf('tail-%d',v)) FROM wide_pressure", -1, &p, 0)
      ==SQLITE_OK);
  check("compacted prefixes reload complete rows", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==4096 && sqlite3_column_int(p, 1)==4096
      && sqlite3_column_int(p, 2)==4096);
  check("finish compacted prefix fallback", sqlite3_finalize(p)==SQLITE_OK);
  execSql(db, "PRAGMA cache_size=-4096");
  clearNodes(pCache);
  for(i=0; i<3; i++){
    nRead = 0;
    execSql(db, "SELECT sum(pad=printf('%080d',v)) FROM wide_pressure");
  }
  check("larger prefixes survive without pressure", nRead==0);
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "DROP TABLE wide_pressure; PRAGMA cache_size=-65536");
}

static void narrowScalarScan(sqlite3 *db, int nPayload){
  sqlite3_stmt *p = 0;
  check("prepare narrow scalar scan", sqlite3_prepare_v2(db,
      "SELECT sum(v),sum(length(b)) FROM narrow NOT INDEXED",
      -1, &p, 0)==SQLITE_OK);
  check("narrow scalar result", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==8390656
      && sqlite3_column_int(p, 1)==4096*nPayload);
  check("finish narrow scalar scan", sqlite3_finalize(p)==SQLITE_OK);
}

static void testNarrowPrefixes(sqlite3 *db, const char *zKey, int nPayload){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  sqlite3_stmt *p = 0;
  char *zSql = sqlite3_mprintf(
      "CREATE TABLE narrow(id %s PRIMARY KEY,v INTEGER,b BLOB,tail TEXT);"
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<4096)"
      " INSERT INTO narrow SELECT i,i,CAST(printf('%%0*d',%d,i) AS BLOB),"
      " printf('tail-%%d',i) FROM c; PRAGMA cache_size=-512", zKey, nPayload);
  int nCold;
  execSql(db, zSql);
  sqlite3_free(zSql);
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  narrowScalarScan(db, nPayload);
  nCold = nRead;
  narrowScalarScan(db, nPayload);
  nRead = 0;
  narrowScalarScan(db, nPayload);
  check("narrow prefixes retain metadata without payload reads",
      nCold>0 && nRead==0);
  check("narrow prefix cache accounting", cacheBytes(pCache)==pCache->nByte
      && budgetMatches(db, 512*1024));
  zSql = sqlite3_mprintf(
      "SELECT sum(b=CAST(printf('%%0*d',%d,v) AS BLOB)),"
      " sum(tail=printf('tail-%%d',v)) FROM narrow", nPayload);
  check("prepare narrow full values", sqlite3_prepare_v2(db, zSql,
      -1, &p, 0)==SQLITE_OK);
  sqlite3_free(zSql);
  check("narrow full values and trailing columns", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==4096 && sqlite3_column_int(p, 1)==4096);
  check("finish narrow full values", sqlite3_finalize(p)==SQLITE_OK);
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "DROP TABLE narrow; PRAGMA cache_size=-65536");
}

static void testShortPrefixWriteScan(sqlite3 *db){
  ProllyCache *pCache = doltliteGetCache(db);
  ChunkStore *pStore = doltliteGetChunkStore(db);
  sqlite3_io_methods methods;
  sqlite3_stmt *p = 0;
  int i;
  execSql(db,
      "CREATE TABLE prefix_delete(id TEXT PRIMARY KEY,seq INTEGER NOT NULL,"
      " grp INTEGER NOT NULL,v INTEGER NOT NULL,tag TEXT NOT NULL,"
      " payload BLOB NOT NULL);"
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<16384)"
      " INSERT INTO prefix_delete SELECT printf('%016x',i),i,i%256,"
      " (i*7919)%1000000,printf('tag-%08x',i%10000),"
      " CAST(printf('%0256d',i) AS BLOB) FROM c;"
      "CREATE INDEX prefix_delete_gv ON prefix_delete(grp,v);"
      "PRAGMA cache_size=-4096");
  pReadMethods = pStore->file.pFile->pMethods;
  methods = *pReadMethods;
  methods.xRead = countedRead;
  pStore->file.pFile->pMethods = &methods;
  clearNodes(pCache);
  for(i=0; i<3; i++){
    execSql(db, "BEGIN;DELETE FROM prefix_delete WHERE seq%32=0");
    nRead = nBatchRead = 0;
    execSql(db, "UPDATE prefix_delete SET v=v+1 WHERE seq%32=0");
    check("short prefix fallback preserves batched write-scan reads",
        nRead>0 && nBatchRead>nRead/2);
    check("empty write scan changes no rows", sqlite3_changes(db)==0);
    check("prepare scan after prefix fallback", sqlite3_prepare_v2(db,
        "SELECT count(*),sum(v),sum(seq),sum(length(payload))"
        " FROM prefix_delete", -1, &p, 0)==SQLITE_OK);
    check("scan after prefix fallback result", sqlite3_step(p)==SQLITE_ROW
        && sqlite3_column_int(p, 0)==15872
        && sqlite3_column_int64(p, 1)==7924494656LL
        && sqlite3_column_int64(p, 2)==130023424
        && sqlite3_column_int(p, 3)==4063232);
    check("finish scan after prefix fallback", sqlite3_finalize(p)==SQLITE_OK);
    check("write-scan prefix accounting", cacheBytes(pCache)==pCache->nByte
        && budgetMatches(db, 4096*1024));
    execSql(db, "ROLLBACK");
  }
  pStore->file.pFile->pMethods = pReadMethods;
  execSql(db, "DROP TABLE prefix_delete; PRAGMA cache_size=-65536");
}

static ProllyHash nodeHash(int id){
  ProllyHash hash;
  memset(&hash, 0, sizeof(hash));
  memcpy(hash.data, &id, sizeof(id));
  return hash;
}

static ProllyCacheEntry *putNode(ProllyCache *pCache, int id, int nValue){
  ProllyNodeBuilder b;
  ProllyHash hash = nodeHash(id);
  u8 key[8];
  u8 *pValue = sqlite3_malloc(nValue);
  u8 *pData = 0;
  int nData = 0;
  int rc;
  memset(pValue, 'x', nValue);
  prollyEncodeIntKey(id, key);
  prollyNodeBuilderInit(&b, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAdd(&b, key, sizeof(key), pValue, nValue);
  if( rc==SQLITE_OK ) rc = prollyNodeBuilderFinish(&b, &pData, &nData);
  prollyNodeBuilderFree(&b);
  sqlite3_free(pValue);
  if( rc!=SQLITE_OK ){
    check("build node", 0);
    return 0;
  }
  return prollyCachePutOwned(pCache, &hash, pData, nData, &rc);
}

static int hasNode(ProllyCache *pCache, int id){
  ProllyHash hash = nodeHash(id);
  ProllyCacheEntry *p = prollyCacheGet(pCache, &hash);
  if( p ) prollyCacheRelease(pCache, p);
  return p!=0;
}

static ProllyCacheEntry *putInternalNode(ProllyCache *pCache, int id){
  ProllyNodeBuilder builder;
  ProllyHash hash = nodeHash(id);
  u8 key[8];
  u8 *pData = 0;
  int nData = 0;
  int rc = SQLITE_OK;
  int i;
  prollyNodeBuilderInit(&builder, 1, PROLLY_NODE_INTKEY);
  for(i=0; i<32 && rc==SQLITE_OK; i++){
    prollyEncodeIntKey(id*100+i, key);
    rc = prollyNodeBuilderAdd(&builder, key, sizeof(key), hash.data,
                              PROLLY_HASH_SIZE);
  }
  if( rc==SQLITE_OK ) rc = prollyNodeBuilderFinish(&builder, &pData, &nData);
  prollyNodeBuilderFree(&builder);
  check("build internal node", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return 0;
  return prollyCachePutOwned(pCache, &hash, pData, nData, &rc);
}

static void testInternalNodes(void){
  ProllyCache cache;
  ProllyCacheEntry *p;
  ProllyCacheEntry *aPinned[4];
  int i;
  check("init internal node cache", prollyCacheInit(&cache, 4096)==SQLITE_OK);
  p = putInternalNode(&cache, 1);
  check("cache internal node", p!=0);
  if( !p ){ prollyCacheFree(&cache); return; }
  prollyCacheRelease(&cache, p);
  for(i=2; i<=6; i++){
    p = putNode(&cache, i, 1200);
    check("insert leaf under pressure", p!=0);
    if( p ) prollyCacheRelease(&cache, p);
  }
  check("internal node survives leaf churn", hasNode(&cache, 1));
  check("internal preference stays within budget", cache.nByte<=4096
      && cache.nByte==cacheBytes(&cache));
  for(i=7; i<80; i++){
    p = putNode(&cache, i, 1200);
    if( p ) prollyCacheRelease(&cache, p);
  }
  check("unused internal node eventually evicted", !hasNode(&cache, 1));
  for(i=80; i<100; i++){
    p = putInternalNode(&cache, i);
    check("all-internal cache admits nodes", p!=0);
    if( p ) prollyCacheRelease(&cache, p);
  }
  check("all-internal cache remains bounded", cache.nByte<=4096
      && cache.nByte==cacheBytes(&cache));
  prollyCacheFree(&cache);

  check("init pinned internal cache", prollyCacheInit(&cache, 16384)==SQLITE_OK);
  for(i=0; i<4; i++){
    aPinned[i] = putInternalNode(&cache, i);
    check("pin internal node", aPinned[i]!=0);
  }
  prollyCacheSetBudget(&cache, 4096);
  check("pinned internal nodes exceed reduced budget", cache.nByte>4096);
  for(i=0; i<4; i++){
    if( aPinned[i] ){
      check("pinned internal remains readable",
            prollyNodeIntKey(&aPinned[i]->node, 0)==i*100);
      prollyCacheRelease(&cache, aPinned[i]);
    }
  }
  check("internal nodes honor budget after unpin", cache.nByte<=4096
      && cache.nByte==cacheBytes(&cache));
  prollyCacheFree(&cache);
}

static void testNodes(void){
  ProllyCache cache;
  ProllyCacheEntry *a, *b, *c;
  int i;
  int nHit = 0;
  check("init small cache", prollyCacheInit(&cache, 4096)==SQLITE_OK);
  a = putNode(&cache, 1, 1200);
  b = putNode(&cache, 2, 1200);
  check("two pinned nodes", a && b);
  if( !a || !b ) return;
  prollyCacheRelease(&cache, a);
  prollyCacheRelease(&cache, b);
  check("refresh oldest", hasNode(&cache, 1));
  c = putNode(&cache, 3, 1200);
  check("third node", c!=0);
  if( !c ) return;
  prollyCacheRelease(&cache, c);
  check("evict least recently used", !hasNode(&cache, 2)
      && hasNode(&cache, 1) && hasNode(&cache, 3));
  check("account allocations", cache.nByte==cacheBytes(&cache));
  a = putNode(&cache, 1, 1200);
  b = putNode(&cache, 1, 1200);
  check("duplicate hash shares pinned entry", a==b && a->nRef==2);
  c = putNode(&cache, 4, 16000);
  check("oversized pinned node", c && cache.nByte>cache.nMaxByte);
  check("pinned node survives eviction", prollyNodeIntKey(&a->node, 0)==1);
  prollyCacheRelease(&cache, a);
  check("second reference remains valid", prollyNodeIntKey(&b->node, 0)==1);
  prollyCacheRelease(&cache, c);
  check("oversized node released", !hasNode(&cache, 4)
      && cache.nByte<=cache.nMaxByte);
  prollyCacheRelease(&cache, b);
  check("released accounting", cache.nByte==cacheBytes(&cache));
  prollyCacheFree(&cache);

  check("init large cache", prollyCacheInit(&cache, 16*1024*1024)==SQLITE_OK);
  for(i=0; i<17000; i++){
    a = putNode(&cache, i, 16);
    if( !a ) break;
    prollyCacheRelease(&cache, a);
  }
  check("retain more than old entry ceiling", cache.nUsed==17000);
  for(i=0; i<17000; i++) nHit += hasNode(&cache, i);
  check("reuse entire working set", nHit==17000);
  check("hash grows with actual usage", cache.nBucket>16);
  check("large accounting", cache.nByte==cacheBytes(&cache));
  prollyCacheSetBudget(&cache, 4096);
  check("shrink entries and hash storage", cache.nByte<=4096
      && cache.nByte==cacheBytes(&cache));
  check("most recent node survives rehash", hasNode(&cache, 16999));
  prollyCacheFree(&cache);
}

int main(void){
  char zPath[160];
  sqlite3 *db = 0;
  ProllyCache *pCache;
  sqlite3_stmt *p = 0;
  int i;
  sqlite3_snprintf(sizeof(zPath), zPath, "/tmp/prolly-cache-budget-%d.db",
                   (int)getpid());
  unlink(zPath);
  check("open", sqlite3_open(zPath, &db)==SQLITE_OK);
  execSql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
      "WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c"
      " WHERE i<100000) INSERT INTO t SELECT i, printf('%0100d',i) FROM c");
  check("close seed", sqlite3_close(db)==SQLITE_OK);
  check("reopen", sqlite3_open(zPath, &db)==SQLITE_OK);
  pCache = doltliteGetCache(db);
  scan(db);
  check("default cache retains scan", cacheBytes(pCache)>8*1024*1024);
  check("default budget", budgetMatches(db, 64*1024*1024));
  check("index cache allocated lazily within its reservation",
      indexCacheBytes(doltliteGetChunkStore(db))>0
      && indexCacheBytes(doltliteGetChunkStore(db))
         <=64*1024*1024-pCache->nMaxByte);
  testReadAhead(db);
  testCachedReadAhead(db);
  testLargeScans(db);
  testNearBudgetScans(db);
  testWidePrefixes(db);
  testWidePrefixPressure(db);
  testNarrowPrefixes(db, "INTEGER", 256);
  testNarrowPrefixes(db, "TEXT", 1024);
  testShortPrefixWriteScan(db);
  testReload(db);
  scan(db);
  check("reloaded cache respects default budget", budgetMatches(db, 64*1024*1024));
  {
    i64 nBudget = pCache->nMaxByte;
    int nSlot = doltliteGetChunkStore(db)->nIndexCacheSlot;
    check("release cache memory", sqlite3_db_release_memory(db)==SQLITE_OK);
    check("release frees unpinned node and index caches",
        pCache->nUsed==0 && pCache->nByte<=4096
        && indexCacheBytes(doltliteGetChunkStore(db))==0);
    check("release preserves configured budgets", pCache->nMaxByte==nBudget
        && doltliteGetChunkStore(db)->nIndexCacheSlot==nSlot);
    scan(db);
    check("cache refills after release", cacheBytes(pCache)>8*1024*1024
        && budgetMatches(db, 64*1024*1024));
    execSql(db, "PRAGMA shrink_memory");
    check("pragma releases both caches", pCache->nUsed==0
        && indexCacheBytes(doltliteGetChunkStore(db))==0);
  }
  execSql(db, "PRAGMA cache_size=-32768");
  scan(db);
  check("larger cache retains scan", cacheBytes(pCache)>8*1024*1024);
  execSql(db, "PRAGMA cache_size=100");
  check("positive budget shrinks immediately", combinedCacheBytes(db)<=100*4096);
  scan(db);
  check("positive budget bounds scan", combinedCacheBytes(db)<=100*4096);
  execSql(db, "PRAGMA cache_size=-2000");
  scan(db);
  check("negative budget bounds scan", combinedCacheBytes(db)<=2000*1024);
  check("prepare pinned scan", sqlite3_prepare_v2(db,
      "SELECT id, v FROM t ORDER BY id", -1, &p, 0)==SQLITE_OK);
  check("pin first row", sqlite3_step(p)==SQLITE_ROW);
  check("release with pinned cursor", sqlite3_db_release_memory(db)==SQLITE_OK);
  check("pinned row survives release", sqlite3_column_int(p, 0)==1
      && sqlite3_column_bytes(p, 1)==100);
  execSql(db, "PRAGMA cache_size=0");
  check("pinned row survives shrink", sqlite3_column_int(p, 0)==1
      && sqlite3_column_bytes(p, 1)==100);
  for(i=2; sqlite3_step(p)==SQLITE_ROW; i++){
    if( sqlite3_column_int(p, 0)!=i || sqlite3_column_bytes(p, 1)!=100 ) break;
  }
  check("pinned scan remains correct", i==100001);
  check("finalize pinned scan", sqlite3_finalize(p)==SQLITE_OK);
  check("minimum budget after unpin", combinedCacheBytes(db)<=4096);
  execSql(db, "PRAGMA cache_size=100; PRAGMA page_size=8192");
  check("positive budget follows page size", budgetMatches(db, 100*8192));
  execSql(db, "PRAGMA cache_size=-2000; PRAGMA page_size=4096");
  check("KiB budget is independent of page size",
      budgetMatches(db, 2000*1024));
  execSql(db, "PRAGMA cache_size=-2147483648");
  check("minimum signed integer KiB budget",
      budgetMatches(db, 2147483648LL*1024));
  check("large budget allocates lazily", combinedCacheBytes(db)<=4096);
  execSql(db, "PRAGMA page_size=8192; PRAGMA cache_size=2147483647");
  check("maximum signed integer page budget",
      budgetMatches(db, 2147483647LL*8192));
  execSql(db, "PRAGMA cache_size=-1");
  check("tiny KiB budget uses minimum", pCache->nMaxByte==4096);
  check("tiny budget disables index cache",
      doltliteGetChunkStore(db)->nIndexCacheSlot==0
      && indexCacheBytes(doltliteGetChunkStore(db))==0);
  testReload(db);
  scan(db);
  check("reloaded cache respects tiny budget", budgetMatches(db, 4096));

  check("close", sqlite3_close(db)==SQLITE_OK);
  unlink(zPath);
  testNodes();
  testInternalNodes();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail!=0;
}
