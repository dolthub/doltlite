#include "prolly_btree_int.h"
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

static sqlite3_int64 cacheBytes(ProllyCache *pCache){
  ProllyCacheEntry *p;
  sqlite3_int64 n = sqlite3_msize(pCache->aBucket);
  for(p=pCache->lruHead.pLruNext; p!=&pCache->lruTail; p=p->pLruNext){
    n += sqlite3_msize(p) + sqlite3_msize(p->pAlloc);
  }
  return n;
}

static void scan(sqlite3 *db){
  sqlite3_stmt *p = 0;
  check("prepare scan", sqlite3_prepare_v2(db,
      "SELECT count(v), sum(length(v)), sum(id) FROM t", -1, &p, 0)
      ==SQLITE_OK);
  check("scan result", sqlite3_step(p)==SQLITE_ROW
      && sqlite3_column_int(p, 0)==100000
      && sqlite3_column_int(p, 1)==10000000
      && sqlite3_column_int64(p, 2)==5000050000LL);
  check("finish scan", sqlite3_finalize(p)==SQLITE_OK);
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

static void testReadBuffer(
  ChunkStore *pStore, const ProllyHash *pHash, const u8 *pData, int nData,
  int bSparse, int bDisk, int bExpectSparse
){
  ProllyCache cache;
  ProllyCacheEntry *pEntry;
  ChunkBuffer buffer;
  u8 *pAlloc;
  int rc;
  int i;
  check("init read cache", prollyCacheInit(&cache, 4096)==SQLITE_OK);
  rc = chunkStoreGetBuffer(pStore, pHash, bSparse,
                           PROLLY_NODE_BUFFER_SLOP, &buffer);
  check("read chunk buffer", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ prollyCacheFree(&cache); return; }
  check("chunk buffer contents", buffer.nData==nData
      && memcmp(buffer.pData, pData, buffer.nDataPhys)==0);
  check("read retains disk header", buffer.pData==buffer.pAlloc+(bDisk ? 4 : 0));
  check("read reserves parser padding", sqlite3_msize(buffer.pAlloc)
      >=(u64)(buffer.pData-buffer.pAlloc)+buffer.nDataPhys
        +PROLLY_NODE_BUFFER_SLOP);
  check("sparse read omits zero tail", bExpectSparse
      ? buffer.nDataPhys<nData : buffer.nDataPhys==nData);
  pAlloc = buffer.pAlloc;
  pEntry = prollyCachePutBufferOwned(&cache, pHash, &buffer, &rc);
  check("adopt read buffer", pEntry!=0 && rc==SQLITE_OK);
  if( pEntry ){
    check("cache retains original allocation", pEntry->pAlloc==pAlloc);
    check("cache parses body after header", prollyNodeIntKey(&pEntry->node, 0)==42);
    for(i=0; i<PROLLY_NODE_BUFFER_SLOP; i++){
      if( pEntry->pData[pEntry->nDataPhys+i]!=0 ) break;
    }
    check("parser padding is zero", i==PROLLY_NODE_BUFFER_SLOP);
    check("sparse node remains transient", pEntry->bTransient==bExpectSparse);
    check("read cache accounting", cache.nByte==cacheBytes(&cache));
    if( !bExpectSparse ){
      ProllyCacheEntry *pDuplicate;
      rc = chunkStoreGetBuffer(pStore, pHash, 0,
                               PROLLY_NODE_BUFFER_SLOP, &buffer);
      check("read duplicate buffer", rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        pDuplicate = prollyCachePutBufferOwned(&cache, pHash, &buffer, &rc);
        check("duplicate releases allocation base", pDuplicate==pEntry
            && pEntry->nRef==2);
        if( pDuplicate ) prollyCacheRelease(&cache, pDuplicate);
      }
    }
    prollyCacheRelease(&cache, pEntry);
  }
  prollyCacheFree(&cache);
}

static void testReadBuffers(void){
  char zPath[160];
  ChunkStore store;
  ProllyNodeBuilder builder;
  ProllyHash hash;
  ChunkBuffer buffer;
  ProllyCache cache;
  ProllyCacheEntry *pEntry;
  u8 key[8];
  u8 value[1024];
  u8 *pData = 0;
  int nData = 0;
  int rc;
  int phase;
  i64 nBefore = sqlite3_memory_used();
  sqlite3_snprintf(sizeof(zPath), zPath, "/tmp/prolly-read-buffer-%d.db",
                   (int)getpid());
  unlink(zPath);
  memset(value, 0, sizeof(value));
  prollyEncodeIntKey(42, key);
  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAdd(&builder, key, sizeof(key), value, sizeof(value));
  if( rc==SQLITE_OK ) rc = prollyNodeBuilderFinish(&builder, &pData, &nData);
  prollyNodeBuilderFree(&builder);
  check("build read node", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  rc = chunkStoreOpen(&store, sqlite3_vfs_find(0), zPath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("open read store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_free(pData); return; }
  rc = chunkStorePutSparse(&store, pData, nData-sizeof(value), sizeof(value), &hash);
  check("store sparse node", rc==SQLITE_OK);
  for(phase=0; phase<3 && rc==SQLITE_OK; phase++){
    testReadBuffer(&store, &hash, pData, nData, 0, phase!=0, 0);
    testReadBuffer(&store, &hash, pData, nData, 1, phase!=0, phase<2);
    if( phase==0 ){
      int sparse;
      i64 off = store.staging.aPending[0].offset + 4;
      store.staging.pWriteBuf[off] ^= 1;
      for(sparse=0; sparse<2; sparse++){
        rc = chunkStoreGetBuffer(&store, &hash, sparse,
                                 PROLLY_NODE_BUFFER_SLOP, &buffer);
        check("corrupt chunk clears owned buffer", rc==SQLITE_CORRUPT
            && !buffer.pAlloc && !buffer.pData
            && buffer.nData==0 && buffer.nDataPhys==0);
        sqlite3_free(buffer.pAlloc);
      }
      store.staging.pWriteBuf[off] ^= 1;
      rc = chunkStoreCommit(&store);
      check("commit read store", rc==SQLITE_OK);
    }else if( phase==1 ){
      chunkStoreClose(&store);
      rc = chunkStoreOpen(&store, sqlite3_vfs_find(0), zPath,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
      check("reopen read store", rc==SQLITE_OK);
    }
  }
  chunkStoreClose(&store);
  sqlite3_free(pData);
  unlink(zPath);

  check("init corrupt read cache", prollyCacheInit(&cache, 4096)==SQLITE_OK);
  buffer.pAlloc = sqlite3_malloc(32);
  buffer.pData = buffer.pAlloc + 4;
  buffer.nData = buffer.nDataPhys = 8;
  memset(buffer.pAlloc, 0, 32);
  pEntry = prollyCachePutBufferOwned(&cache, &hash, &buffer, &rc);
  check("invalid node releases allocation base", pEntry==0 && rc==SQLITE_CORRUPT);
  prollyCacheFree(&cache);
  check("read buffers release all memory", sqlite3_memory_used()==nBefore);
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
  check("default budget", cacheBytes(pCache)<=64*1024*1024);
  execSql(db, "PRAGMA cache_size=-32768");
  scan(db);
  check("larger cache retains scan", cacheBytes(pCache)>8*1024*1024);
  execSql(db, "PRAGMA cache_size=100");
  check("positive budget shrinks immediately", cacheBytes(pCache)<=100*4096);
  scan(db);
  check("positive budget bounds scan", cacheBytes(pCache)<=100*4096);
  execSql(db, "PRAGMA cache_size=-2000");
  scan(db);
  check("negative budget bounds scan", cacheBytes(pCache)<=2000*1024);
  check("prepare pinned scan", sqlite3_prepare_v2(db,
      "SELECT id, v FROM t ORDER BY id", -1, &p, 0)==SQLITE_OK);
  check("pin first row", sqlite3_step(p)==SQLITE_ROW);
  execSql(db, "PRAGMA cache_size=0");
  check("pinned row survives shrink", sqlite3_column_int(p, 0)==1
      && sqlite3_column_bytes(p, 1)==100);
  for(i=2; sqlite3_step(p)==SQLITE_ROW; i++){
    if( sqlite3_column_int(p, 0)!=i || sqlite3_column_bytes(p, 1)!=100 ) break;
  }
  check("pinned scan remains correct", i==100001);
  check("finalize pinned scan", sqlite3_finalize(p)==SQLITE_OK);
  check("minimum budget after unpin", cacheBytes(pCache)<=4096);
  execSql(db, "PRAGMA cache_size=100; PRAGMA page_size=8192");
  check("positive budget follows page size", pCache->nMaxByte==100*8192);
  execSql(db, "PRAGMA cache_size=-2000; PRAGMA page_size=4096");
  check("KiB budget is independent of page size",
      pCache->nMaxByte==2000*1024);
  execSql(db, "PRAGMA cache_size=-2147483648");
  check("minimum signed integer KiB budget",
      pCache->nMaxByte==2147483648LL*1024);
  check("large budget allocates lazily", cacheBytes(pCache)<=4096);
  execSql(db, "PRAGMA page_size=8192; PRAGMA cache_size=2147483647");
  check("maximum signed integer page budget",
      pCache->nMaxByte==2147483647LL*8192);
  execSql(db, "PRAGMA cache_size=-1");
  check("tiny KiB budget uses minimum", pCache->nMaxByte==4096);

  check("close", sqlite3_close(db)==SQLITE_OK);
  unlink(zPath);
  testNodes();
  testReadBuffers();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail!=0;
}
