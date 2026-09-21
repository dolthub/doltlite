
#ifdef DOLTLITE_PROLLY

#include "prolly_cache.h"
#include <string.h>
#include <assert.h>

/* Trailing zeros so parsing the last cell can over-read one varint (max 9 bytes). */
#define PROLLY_NODE_BUFFER_SLOP 8
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

static void lruInsertHead(ProllyCache *cache, ProllyCacheEntry *pEntry){
  pEntry->pLruNext = cache->lruHead.pLruNext;
  pEntry->pLruPrev = &cache->lruHead;
  cache->lruHead.pLruNext->pLruPrev = pEntry;
  cache->lruHead.pLruNext = pEntry;
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
  pEntry->nData = nData;
  pEntry->nDataPhys = nDataPhys;
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

  return SQLITE_OK;
}

ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash){
  int iBucket;
  ProllyCacheEntry *pEntry;

  if( cache->aBucket==0 ) return 0;

  iBucket = cacheHashBucket(cache, hash);
  pEntry = cache->aBucket[iBucket];

  while( pEntry ){
    if( memcmp(pEntry->hash.data, hash->data, PROLLY_HASH_SIZE)==0 ){

      pEntry->nEvictChance = pEntry->node.level>0
                          ? PROLLY_CACHE_INTERNAL_CHANCES : 0;
      pEntry->nRef++;
      if( pEntry->pLruPrev!=&cache->lruHead ){
        lruRemove(pEntry);
        lruInsertHead(cache, pEntry);
      }
      return pEntry;
    }
    pEntry = pEntry->pHashNext;
  }

  return 0;
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

static ProllyCacheEntry *cacheEvictOne(ProllyCache *cache){
  ProllyCacheEntry *pEntry = cacheEvictionCandidate(cache);
  if( pEntry ){
    lruRemove(pEntry);
    hashRemove(cache, pEntry);
    cache->nByte -= sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData);
    sqlite3_free(pEntry->pData);
    memset(pEntry, 0, sizeof(*pEntry));
    cache->nUsed--;
  }
  return pEntry;
}

static void cacheTrim(ProllyCache *cache, i64 nMaxByte){
  int pass;
  for(pass=0; pass<2 && cache->nByte>nMaxByte; pass++){
    ProllyCacheEntry *pEntry = cache->lruTail.pLruPrev;
    int nVisit = cache->nUsed;
    while( nVisit-- && cache->nByte>nMaxByte && pEntry!=&cache->lruHead ){
      ProllyCacheEntry *pPrev = pEntry->pLruPrev;
      if( pEntry->nRef==0 ){
        if( pEntry->nEvictChance>0 && pass==0 ){
          pEntry->nEvictChance--;
          lruRemove(pEntry);
          lruInsertHead(cache, pEntry);
        }else{
          lruRemove(pEntry);
          hashRemove(cache, pEntry);
          cache->nByte -= sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData);
          cache->nUsed--;
          cacheEntryFree(pEntry);
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
  pEntry->nData = nData;
  pEntry->nDataPhys = nData;
  pEntry->nRef = 1;
  pEntry->bTransient = 0;

  rc = prollyNodeParse(&pEntry->node, pData, nData);
  if( rc!=SQLITE_OK ){
    if( pRc ) *pRc = rc;
    sqlite3_free(pData);
    sqlite3_free(pEntry);
    return 0;
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
  cache->nByte += sqlite3_msize(pEntry) + sqlite3_msize(pEntry->pData);
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

  sqlite3_free(cache->aBucket);
  memset(cache, 0, sizeof(*cache));
}

#endif
