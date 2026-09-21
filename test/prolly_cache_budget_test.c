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

static sqlite3_int64 cacheBytes(ProllyCache *pCache){
  ProllyCacheEntry *p;
  sqlite3_int64 n = sqlite3_msize(pCache->aBucket);
  for(p=pCache->lruHead.pLruNext; p!=&pCache->lruTail; p=p->pLruNext){
    n += sqlite3_msize(p) + sqlite3_msize(p->pData);
  }
  return n;
}

static sqlite3_int64 combinedCacheBytes(sqlite3 *db){
  return cacheBytes(doltliteGetCache(db))
       + csIndexCacheBytes(doltliteGetChunkStore(db));
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
  check("reload discards differently sized index cache", csIndexCacheBytes(cs)==0);
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

static int countedRead(sqlite3_file *pFile, void *pData, int n, sqlite3_int64 off){
  int rc;
  nRead++;
  if( n>16384 ){
    nBatchRead++;
    if( eReadFault==1 ) return SQLITE_IOERR_READ;
  }
  rc = pReadMethods->xRead(pFile, pData, n, off);
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
      csIndexCacheBytes(doltliteGetChunkStore(db))>0
      && csIndexCacheBytes(doltliteGetChunkStore(db))
         <=64*1024*1024-pCache->nMaxByte);
  testReadAhead(db);
  testReload(db);
  scan(db);
  check("reloaded cache respects default budget", budgetMatches(db, 64*1024*1024));
  {
    i64 nBudget = pCache->nMaxByte;
    int nSlot = doltliteGetChunkStore(db)->nIndexCacheSlot;
    check("release cache memory", sqlite3_db_release_memory(db)==SQLITE_OK);
    check("release frees unpinned node and index caches",
        pCache->nUsed==0 && pCache->nByte<=4096
        && csIndexCacheBytes(doltliteGetChunkStore(db))==0);
    check("release preserves configured budgets", pCache->nMaxByte==nBudget
        && doltliteGetChunkStore(db)->nIndexCacheSlot==nSlot);
    scan(db);
    check("cache refills after release", cacheBytes(pCache)>8*1024*1024
        && budgetMatches(db, 64*1024*1024));
    execSql(db, "PRAGMA shrink_memory");
    check("pragma releases both caches", pCache->nUsed==0
        && csIndexCacheBytes(doltliteGetChunkStore(db))==0);
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
      && csIndexCacheBytes(doltliteGetChunkStore(db))==0);
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
