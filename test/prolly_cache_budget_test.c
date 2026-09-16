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
    n += sqlite3_msize(p) + sqlite3_msize(p->pData);
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
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail!=0;
}
