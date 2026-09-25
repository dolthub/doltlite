
#ifdef DOLTLITE_PROLLY

#include "prolly_cache.h"
#include <string.h>
#include <assert.h>

static void cacheTrim(ProllyCache*, i64);

#define PROLLY_CACHE_INTERNAL_CHANCES 8

static int cacheHashBucket(const ProllyCache *cache, const ProllyHash *hash){
  u32 h;
  memcpy(&h, hash->data, sizeof(u32));
  return (int)(h & (u32)(cache->nBucket - 1));
}

static void lruRemove(ProllyCacheEntry *pEntry){
  pEntry->pLruPrev->pLruNext = pEntry->pLruNext;
  pEntry->pLruNext->pLruPrev = pEntry->pLruPrev;
  pEntry->pLruNext = 0;
  pEntry->pLruPrev = 0;
}

/* Packed prefixes share the prefix LRU; decoded buffers can be discarded
** without losing the packed copy. */
static ProllyCacheEntry *lruHeadFor(ProllyCache *cache, ProllyCacheEntry *p){
  return p->pPacked ? (p->pData ? &cache->lruHead : &cache->prefixHead)
       : p->node.nValuePrefix ? &cache->prefixHead : &cache->lruHead;
}

static void lruInsertHead(ProllyCache *cache, ProllyCacheEntry *pEntry){
  ProllyCacheEntry *pHead = lruHeadFor(cache, pEntry);
  pEntry->pLruNext = pHead->pLruNext;
  pEntry->pLruPrev = pHead;
  pHead->pLruNext->pLruPrev = pEntry;
  pHead->pLruNext = pEntry;
}

static void hashRemove(ProllyCache *cache, ProllyCacheEntry *pEntry){
  int iBucket = cacheHashBucket(cache, &pEntry->hash);
  ProllyCacheEntry **pp = &cache->aBucket[iBucket];
  while( *pp ){
    if( *pp==pEntry ){
      *pp = pEntry->pHashNext;
      pEntry->pHashNext = 0;
      return;
    }
    pp = &((*pp)->pHashNext);
  }
}

static void cacheEntryFree(ProllyCacheEntry *pEntry){
  if( pEntry ){
    sqlite3_free(pEntry->pData);
    sqlite3_free(pEntry->pPacked);
    sqlite3_free(pEntry);
  }
}

static ProllyCacheEntry *cacheEntryNewOwned(
  const ProllyHash *hash,
  u8 *pData,
  int nData,
  int nDataPhys,
  int bTransient,
  int *pRc
){
  ProllyCacheEntry *pEntry;
  int rc;

  if( pRc ) *pRc = SQLITE_OK;
  pEntry = (ProllyCacheEntry *)sqlite3_malloc(sizeof(ProllyCacheEntry));
  if( pEntry==0 ){
    sqlite3_free(pData);
    if( pRc ) *pRc = SQLITE_NOMEM;
    return 0;
  }
  memset(pEntry, 0, sizeof(*pEntry));

  {
    u8 *pPadded = (u8*)sqlite3_realloc(
        pData, nDataPhys + PROLLY_NODE_BUFFER_SLOP);
    if( pPadded==0 ){
      sqlite3_free(pData);
      sqlite3_free(pEntry);
      if( pRc ) *pRc = SQLITE_NOMEM;
      return 0;
    }
    pData = pPadded;
    memset(pData + nDataPhys, 0, PROLLY_NODE_BUFFER_SLOP);
  }

  memcpy(pEntry->hash.data, hash->data, PROLLY_HASH_SIZE);
  pEntry->pData = pData;
  pEntry->nRef = 1;
  pEntry->bTransient = bTransient ? 1 : 0;

  rc = prollyNodeParseSparse(&pEntry->node, pData, nData, nDataPhys);
  if( rc!=SQLITE_OK ){
    if( pRc ) *pRc = rc;
    cacheEntryFree(pEntry);
    return 0;
  }

  return pEntry;
}

int prollyCacheInit(ProllyCache *cache, i64 nMaxByte){
  int nBucket = 16;

  memset(cache, 0, sizeof(*cache));
  cache->nMaxByte = MAX(nMaxByte, 4096);
  cache->nUsed = 0;

  cache->nBucket = nBucket;

  cache->aBucket = (ProllyCacheEntry **)sqlite3_malloc(
    sizeof(ProllyCacheEntry *) * nBucket
  );
  if( cache->aBucket==0 ){
    return SQLITE_NOMEM;
  }
  memset(cache->aBucket, 0, sizeof(ProllyCacheEntry *) * nBucket);
  cache->nByte = sqlite3_msize(cache->aBucket);

  cache->lruHead.pLruNext = &cache->lruTail;
  cache->lruHead.pLruPrev = 0;
  cache->lruTail.pLruPrev = &cache->lruHead;
  cache->lruTail.pLruNext = 0;
  cache->prefixHead.pLruNext = &cache->prefixTail;
  cache->prefixHead.pLruPrev = 0;
  cache->prefixTail.pLruPrev = &cache->prefixHead;
  cache->prefixTail.pLruNext = 0;

  return SQLITE_OK;
}

static ProllyCacheEntry *cacheFind(ProllyCache *cache, const ProllyHash *hash){
  ProllyCacheEntry *pEntry;
  if( !cache->aBucket ) return 0;
  pEntry = cache->aBucket[cacheHashBucket(cache, hash)];
  while( pEntry && memcmp(pEntry->hash.data, hash->data, PROLLY_HASH_SIZE)!=0 ){
    pEntry = pEntry->pHashNext;
  }
  return pEntry;
}

static ProllyCacheEntry *cacheGet(
  ProllyCache *cache, const ProllyHash *hash, int bScan, int bPrefix
){
  ProllyCacheEntry *pEntry = cacheFind(cache, hash);
  if( !pEntry ) return 0;
  if( pEntry->node.nValuePrefix && !bPrefix ) return 0;
  if( pEntry->pPacked && !pEntry->pData ){
    ProllyNode node;
    int nPrefix = pEntry->node.nValuePrefix;
    int n = pEntry->node.nDataPhys;
    int nHead = n-pEntry->node.nItems
                *(PROLLY_CACHE_SHARED_PREFIX+PROLLY_NODE_BUFFER_SLOP);
    int nVar = 0, i, j;
    u8 aPos[PROLLY_CACHE_SHARED_PREFIX];
    const u8 *p = pEntry->pPacked+nHead;
    u32 mask = PROLLY_GET_U32(p);
    u8 *pData;
    sqlite3BeginBenignMalloc();
    pData = sqlite3_malloc(n + PROLLY_NODE_BUFFER_SLOP);
    sqlite3EndBenignMalloc();
    if( !pData ) return 0;
    memcpy(pData, pEntry->pPacked, nHead);
    for(i=0; i<PROLLY_CACHE_SHARED_PREFIX; i++){
      if( mask & ((u32)1<<i) ) aPos[nVar++] = i;
    }
    p += 4+PROLLY_CACHE_SHARED_PREFIX;
    for(i=0; i<pEntry->node.nItems; i++){
      u8 *pRow = pData+nHead
               + i*(PROLLY_CACHE_SHARED_PREFIX+PROLLY_NODE_BUFFER_SLOP);
      memcpy(pRow, pEntry->pPacked+nHead+4, PROLLY_CACHE_SHARED_PREFIX);
      for(j=0; j<nVar; j++) pRow[aPos[j]] = *p++;
      memset(pRow+PROLLY_CACHE_SHARED_PREFIX, 0, PROLLY_NODE_BUFFER_SLOP);
    }
    assert( p<=pEntry->pPacked+sqlite3_msize(pEntry->pPacked) );
    memset(pData+n, 0, PROLLY_NODE_BUFFER_SLOP);
    if( prollyNodeParseSparse(&node, pData, pEntry->node.nData, n)
        !=SQLITE_OK ){
      sqlite3_free(pData);
      return 0;
    }
    node.nValuePrefix = nPrefix;
    pEntry->node = node;
    pEntry->pData = pData;
    cache->nByte += sqlite3_msize(pData);
  }
  if( !bScan ) pEntry->bScanOnly = 0;
  pEntry->nEvictChance = pEntry->node.level>0
                      ? PROLLY_CACHE_INTERNAL_CHANCES : 0;
  pEntry->nRef++;
  if( pEntry->pLruPrev!=lruHeadFor(cache, pEntry) ){
    lruRemove(pEntry);
    lruInsertHead(cache, pEntry);
  }
  if( cache->nByte>cache->nMaxByte ) cacheTrim(cache, cache->nMaxByte);
  return pEntry;
}

ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash){
  return cacheGet(cache, hash, 0, 0);
}

ProllyCacheEntry *prollyCacheGetForScan(ProllyCache *cache, const ProllyHash *hash){
  return cacheGet(cache, hash, 1, 0);
}

ProllyCacheEntry *prollyCacheGetPrefix(
  ProllyCache *cache, const ProllyHash *hash, int bScan
){
  return cacheGet(cache, hash, bScan, 1);
}

static int cacheKeepPrefixes(ProllyCache *cache, ProllyCacheEntry *pEntry){
  ProllyNode *pNode = &pEntry->node;
  int nHead, nCompact, nPrefix, nStride, nAverage, i;
  int nBasePrefix;
  u8 *pPacked = 0;
  u8 *pData;
  if( pEntry->pPacked ){
    if( !pEntry->pData ) return 0;
    cache->nByte -= sqlite3_msize(pEntry->pData);
    sqlite3_free(pEntry->pData);
    pEntry->pData = 0;
    lruRemove(pEntry);
    lruInsertHead(cache, pEntry);
    return 1;
  }
  if( !pEntry->bAllowPrefix || pNode->level || pNode->nItems==0 ) return 0;
  if( pNode->nValuePrefix ){
    if( pNode->nValuePrefix!=PROLLY_NODE_VALUE_PREFIX ) return 0;
  }else if( pNode->nDataPhys!=pNode->nData ) return 0;
  nHead = (int)(pNode->pValData - pNode->pData);
  if( nHead>pNode->nData/4 ) return 0;
  nAverage = (pNode->nData-nHead)/pNode->nItems;
  nBasePrefix = pNode->nValuePrefix ? PROLLY_NODE_VALUE_PREFIX/2
              : nAverage>=4096 ? PROLLY_NODE_VALUE_PREFIX
              : nAverage>=512 ? 32 : 16;
  nPrefix = pEntry->bScanOnly && nAverage>=128 && nAverage<=1024
          ? PROLLY_CACHE_SHARED_PREFIX : nBasePrefix;
  nStride = nPrefix + PROLLY_NODE_BUFFER_SLOP;
  nCompact = nHead + pNode->nItems*nStride;
  if( nCompact>pNode->nData/4 && nPrefix!=nBasePrefix ){
    nPrefix = nBasePrefix;
    nStride = nPrefix+PROLLY_NODE_BUFFER_SLOP;
    nCompact = nHead+pNode->nItems*nStride;
  }
  if( nCompact>pNode->nData/4 ) return 0;
  assert( pEntry->nRef==0 );
  if( pEntry->bScanOnly && nPrefix==PROLLY_CACHE_SHARED_PREFIX
   && nAverage>=128 && nAverage<=1024 ){
    u32 mask = 0;
    u8 aDiff[PROLLY_CACHE_SHARED_PREFIX] = {0};
    u8 aPos[PROLLY_CACHE_SHARED_PREFIX];
    u8 aFirst[PROLLY_CACHE_SHARED_PREFIX] = {0};
    int nVar = 0, j, n, nVal, nAvail;
    const u8 *pVal;
    prollyNodeValueSpan(pNode, 0, &pVal, &nVal, &nAvail);
    memcpy(aFirst, pVal, MIN(nAvail, PROLLY_CACHE_SHARED_PREFIX));

    for(i=1; i<pNode->nItems; i++){
      u8 aShort[PROLLY_CACHE_SHARED_PREFIX] = {0};
      prollyNodeValueSpan(pNode, i, &pVal, &nVal, &nAvail);
      if( nAvail<PROLLY_CACHE_SHARED_PREFIX ){
        memcpy(aShort, pVal, nAvail);
        pVal = aShort;
      }
      for(j=0; j<PROLLY_CACHE_SHARED_PREFIX; j++){
        aDiff[j] |= aFirst[j]^pVal[j];
      }
    }
    for(j=0; j<PROLLY_CACHE_SHARED_PREFIX; j++){
      if( aDiff[j] ){
        mask |= (u32)1<<j;
        aPos[nVar++] = j;
      }
    }
    n = nHead+4+PROLLY_CACHE_SHARED_PREFIX+pNode->nItems*nVar;
    sqlite3BeginBenignMalloc();
    pPacked = n<=nCompact*3/4 ? sqlite3_malloc(n) : 0;
    sqlite3EndBenignMalloc();
    if( pPacked ){
      u8 *p = pPacked+nHead+4+PROLLY_CACHE_SHARED_PREFIX;
      memcpy(pPacked, pEntry->pData, nHead);
      PROLLY_PUT_U32(pPacked+nHead, mask);
      memcpy(pPacked+nHead+4, aFirst, PROLLY_CACHE_SHARED_PREFIX);
      for(i=0; i<pNode->nItems; i++){
        prollyNodeValueSpan(pNode, i, &pVal, &nVal, &nAvail);
        for(j=0; j<nVar; j++) *p++ = aPos[j]<nAvail ? pVal[aPos[j]] : 0;
      }
    }
  }
  pData = pPacked;
  if( !pPacked ){
    nPrefix = nBasePrefix;
    nStride = nPrefix+PROLLY_NODE_BUFFER_SLOP;
    nCompact = nHead+pNode->nItems*nStride;
    sqlite3BeginBenignMalloc();
    pData = sqlite3_malloc(nCompact+PROLLY_NODE_BUFFER_SLOP);
    sqlite3EndBenignMalloc();
    if( !pData ) return 0;
    memcpy(pData, pEntry->pData, nHead);
    for(i=0; i<pNode->nItems; i++){
      const u8 *pVal;
      int nVal, nAvail;
      u8 *pDest = pData+nHead+i*nStride;
      prollyNodeValueSpan(pNode, i, &pVal, &nVal, &nAvail);
      nVal = MIN(nAvail, nPrefix);
      memcpy(pDest, pVal, nVal);
      memset(pDest+nVal, 0, nStride-nVal);
    }
    memset(pData+nCompact, 0, PROLLY_NODE_BUFFER_SLOP);
  }
  pNode->aKeyOff = (const u32*)(pData + ((const u8*)pNode->aKeyOff-pNode->pData));
  pNode->aValOff = (const u32*)(pData + ((const u8*)pNode->aValOff-pNode->pData));
  pNode->pKeyData = pData + (pNode->pKeyData-pNode->pData);
  pNode->pValData = pData + nHead;
  pNode->pData = pData;
  pNode->nDataPhys = nCompact;
  pNode->nValuePrefix = nPrefix;
  pEntry->nEvictChance = 0;
  cache->nByte += (i64)sqlite3_msize(pData)-(i64)sqlite3_msize(pEntry->pData);
  sqlite3_free(pEntry->pData);
  pEntry->pData = pData;
  if( pPacked ){
    pEntry->pPacked = pPacked;
    cache->nSharedPrefix++;
    pEntry->pData = 0;
  }

  lruRemove(pEntry);
  lruInsertHead(cache, pEntry);
  return 1;
}

void prollyCacheReleaseScan(ProllyCache *cache, ProllyCacheEntry *entry){
  if( entry->bScanOnly && !entry->bTransient && entry->nRef==1
   && entry->node.level==0 && !entry->node.nValuePrefix ){
    lruRemove(entry);
    entry->pLruNext = &cache->lruTail;
    entry->pLruPrev = cache->lruTail.pLruPrev;
    cache->lruTail.pLruPrev->pLruNext = entry;
    cache->lruTail.pLruPrev = entry;
  }
  prollyCacheRelease(cache, entry);
}

static ProllyCacheEntry *cacheEvictionCandidate(ProllyCache *cache){
  ProllyCacheEntry *pEntry = cache->lruTail.pLruPrev;
  ProllyCacheEntry *pFallback = 0;
  int nVisit = cache->nUsed;
  while( nVisit-- && pEntry!=&cache->lruHead ){
    ProllyCacheEntry *pPrev = pEntry->pLruPrev;
    if( pEntry->nRef==0 ){
      if( pEntry->nEvictChance==0 ) return pEntry;
      if( !pFallback ) pFallback = pEntry;
      pEntry->nEvictChance--;
      lruRemove(pEntry);
      lruInsertHead(cache, pEntry);
    }
    pEntry = pPrev;
  }
  return pFallback;
}

static ProllyCacheEntry *cachePrefixCandidate(ProllyCache *cache){
  ProllyCacheEntry *pEntry = cache->prefixTail.pLruPrev;
  while( pEntry!=&cache->prefixHead ){
    if( pEntry->nRef==0 ) return pEntry;
    pEntry = pEntry->pLruPrev;
  }
  return 0;
}

static ProllyCacheEntry *cacheEvictOne(ProllyCache *cache){
  ProllyCacheEntry *pEntry = cacheEvictionCandidate(cache);
  if( !pEntry || pEntry->nEvictChance>0 ){
    ProllyCacheEntry *pPrefix = cachePrefixCandidate(cache);
    if( pPrefix ) pEntry = pPrefix;
  }
  if( pEntry && cacheKeepPrefixes(cache, pEntry) ) return 0;
  if( pEntry ){
    lruRemove(pEntry);
    hashRemove(cache, pEntry);
    cache->nByte -= sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData)
                + sqlite3_msize(pEntry->pPacked);
    sqlite3_free(pEntry->pData);
    if( pEntry->pPacked ) cache->nSharedPrefix--;
    sqlite3_free(pEntry->pPacked);
    memset(pEntry, 0, sizeof(*pEntry));
    cache->nUsed--;
  }
  return pEntry;
}

static void cacheEvictEntry(ProllyCache *cache, ProllyCacheEntry *pEntry){
  lruRemove(pEntry);
  hashRemove(cache, pEntry);
  cache->nByte -= sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData)
                + sqlite3_msize(pEntry->pPacked);
  cache->nUsed--;
  if( pEntry->pPacked ) cache->nSharedPrefix--;
  cacheEntryFree(pEntry);
}

static void cacheTrimPrefixes(ProllyCache *cache, i64 nMaxByte){
  ProllyCacheEntry *pEntry = cache->prefixTail.pLruPrev;
  while( cache->nByte>nMaxByte && pEntry!=&cache->prefixHead ){
    ProllyCacheEntry *pPrev = pEntry->pLruPrev;
    if( pEntry->nRef==0
     && (nMaxByte==0 || !cacheKeepPrefixes(cache, pEntry)) ){
      cacheEvictEntry(cache, pEntry);
    }
    pEntry = pPrev;
  }
}

static void cacheTrim(ProllyCache *cache, i64 nMaxByte){
  int pass;
  for(pass=0; pass<2 && cache->nByte>nMaxByte; pass++){
    ProllyCacheEntry *pEntry = cache->lruTail.pLruPrev;
    int nVisit = cache->nUsed;
    if( pass==1 ){
      cacheTrimPrefixes(cache, nMaxByte);
      if( cache->nByte<=nMaxByte ) break;
    }
    while( nVisit-- && cache->nByte>nMaxByte && pEntry!=&cache->lruHead ){
      ProllyCacheEntry *pPrev = pEntry->pLruPrev;
      if( pEntry->nRef==0 ){
        if( pEntry->nEvictChance>0 && pass==0 ){
          pEntry->nEvictChance--;
          lruRemove(pEntry);
          lruInsertHead(cache, pEntry);
        }else if( nMaxByte==0 || !cacheKeepPrefixes(cache, pEntry) ){
          cacheEvictEntry(cache, pEntry);
        }
      }
      pEntry = pPrev;
    }
  }
}

static void cacheRehash(ProllyCache *cache, int nBucket){
  ProllyCacheEntry **aBucket;
  ProllyCacheEntry *pEntry;
  sqlite3BeginBenignMalloc();
  aBucket = sqlite3_malloc64((u64)nBucket * sizeof(*aBucket));
  sqlite3EndBenignMalloc();
  if( aBucket==0 ) return;
  memset(aBucket, 0, (size_t)nBucket * sizeof(*aBucket));
  cache->nByte += (i64)sqlite3_msize(aBucket)
               - (i64)sqlite3_msize(cache->aBucket);
  sqlite3_free(cache->aBucket);
  cache->aBucket = aBucket;
  cache->nBucket = nBucket;
  for(pEntry=cache->lruHead.pLruNext; pEntry!=&cache->lruTail;
      pEntry=pEntry->pLruNext){
    int iBucket = cacheHashBucket(cache, &pEntry->hash);
    pEntry->pHashNext = cache->aBucket[iBucket];
    cache->aBucket[iBucket] = pEntry;
  }
  for(pEntry=cache->prefixHead.pLruNext; pEntry!=&cache->prefixTail;
      pEntry=pEntry->pLruNext){
    int iBucket = cacheHashBucket(cache, &pEntry->hash);
    pEntry->pHashNext = cache->aBucket[iBucket];
    cache->aBucket[iBucket] = pEntry;
  }
}

void prollyCacheShrink(ProllyCache *cache){
  cacheTrim(cache, 0);
  if( cache->nUsed==0 && cache->nBucket>16 ) cacheRehash(cache, 16);
}

void prollyCacheSetBudget(ProllyCache *cache, i64 nMaxByte){
  int nBucket = cache->nBucket;
  cache->nMaxByte = MAX(nMaxByte, 4096);
  while( nBucket>16
      && (i64)nBucket*sizeof(*cache->aBucket)>cache->nMaxByte/16 ){
    nBucket /= 2;
  }
  if( nBucket!=cache->nBucket ){
    cacheTrim(cache, cache->nMaxByte + sqlite3_msize(cache->aBucket)
                    - (i64)nBucket*sizeof(*cache->aBucket));
    cacheRehash(cache, nBucket);
  }
  cacheTrim(cache, cache->nMaxByte);
}

ProllyCacheEntry *prollyCachePutOwned(
  ProllyCache *cache,
  const ProllyHash *hash,
  u8 *pData,
  int nData,
  int *pRc
){
  int iBucket;
  ProllyCacheEntry *pEntry;
  int rc;

  if( pRc ) *pRc = SQLITE_OK;

  pEntry = prollyCacheGet(cache, hash);
  if( pEntry ){
    sqlite3_free(pData);
    return pEntry;
  }

  pEntry = 0;
  if( cache->nByte + nData + PROLLY_NODE_BUFFER_SLOP
      + sizeof(ProllyCacheEntry)>cache->nMaxByte ){
    pEntry = cacheEvictOne(cache);
  }

  if( pEntry==0 ){
    pEntry = (ProllyCacheEntry *)sqlite3_malloc(sizeof(ProllyCacheEntry));
    if( pEntry==0 ){
      sqlite3_free(pData);
      if( pRc ) *pRc = SQLITE_NOMEM;
      return 0;
    }
    memset(pEntry, 0, sizeof(*pEntry));
  }

  {
    u8 *pPadded = (u8*)sqlite3_realloc(pData, nData + PROLLY_NODE_BUFFER_SLOP);
    if( pPadded==0 ){
      sqlite3_free(pData);
      sqlite3_free(pEntry);
      if( pRc ) *pRc = SQLITE_NOMEM;
      return 0;
    }
    pData = pPadded;
    memset(pData + nData, 0, PROLLY_NODE_BUFFER_SLOP);
  }

  memcpy(pEntry->hash.data, hash->data, PROLLY_HASH_SIZE);
  pEntry->pData = pData;
  pEntry->nRef = 1;
  pEntry->bTransient = 0;

  rc = prollyNodeParse(&pEntry->node, pData, nData);
  if( rc!=SQLITE_OK ){
    if( pRc ) *pRc = rc;
    sqlite3_free(pData);
    sqlite3_free(pEntry);
    return 0;
  }

  {
    ProllyCacheEntry *pOld = cacheFind(cache, hash);
    if( pOld ){
      assert( pOld->node.nValuePrefix );
      pOld->nRef++;
      lruRemove(pOld);
      hashRemove(cache, pOld);
      cache->nByte -= sqlite3_msize(pOld) + sqlite3_msize(pOld->pData)
                  + sqlite3_msize(pOld->pPacked);
      cache->nUsed--;
      if( pOld->pPacked ) cache->nSharedPrefix--;
      /* Other cursors may still borrow the prefix buffer. */
      pOld->bTransient = 1;
      prollyCacheRelease(cache, pOld);
    }
  }

  pEntry->nEvictChance = pEntry->node.level>0
                      ? PROLLY_CACHE_INTERNAL_CHANCES : 0;
  if( cache->nUsed/2>=cache->nBucket && cache->nBucket<0x40000000
      && (i64)cache->nBucket*2*sizeof(*cache->aBucket)<=cache->nMaxByte/16 ){
    cacheRehash(cache, cache->nBucket*2);
  }
  iBucket = cacheHashBucket(cache, hash);
  pEntry->pHashNext = cache->aBucket[iBucket];
  cache->aBucket[iBucket] = pEntry;

  lruInsertHead(cache, pEntry);

  cache->nUsed++;
  cache->nByte += sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData)
                + sqlite3_msize(pEntry->pPacked);
  cacheTrim(cache, cache->nMaxByte);
  return pEntry;
}

ProllyCacheEntry *prollyCachePutTransientOwned(
  const ProllyHash *hash,
  u8 *pData,
  int nData,
  int nDataPhys,
  int *pRc
){
  return cacheEntryNewOwned(hash, pData, nData, nDataPhys, 1, pRc);
}

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry){
  assert( entry->nRef>0 );
  entry->nRef--;
  if( entry->nRef==0 && entry->bTransient ){
    cacheEntryFree(entry);
  }else if( entry->nRef==0 && cache->nByte>cache->nMaxByte ){
    cacheTrim(cache, cache->nMaxByte);
  }
}

void prollyCacheFree(ProllyCache *cache){
  ProllyCacheEntry *pEntry;
  ProllyCacheEntry *pNext;

  if( cache->aBucket==0 ) return;

  pEntry = cache->lruHead.pLruNext;
  while( pEntry!=&cache->lruTail ){
    pNext = pEntry->pLruNext;
    assert( pEntry->nRef==0 );
    cacheEntryFree(pEntry);
    pEntry = pNext;
  }
  pEntry = cache->prefixHead.pLruNext;
  while( pEntry!=&cache->prefixTail ){
    pNext = pEntry->pLruNext;
    assert( pEntry->nRef==0 );
    cacheEntryFree(pEntry);
    pEntry = pNext;
  }

  sqlite3_free(cache->aBucket);
  memset(cache, 0, sizeof(*cache));
}

#endif
