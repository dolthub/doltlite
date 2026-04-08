
#ifdef DOLTLITE_PROLLY

#include "prolly_cache.h"
#include <string.h>
#include <assert.h>

static int cacheHashBucket(const ProllyCache *cache, const ProllyHash *hash){
  u32 h;
  memcpy(&h, hash->data, sizeof(u32));
  return (int)(h % (u32)cache->nBucket);
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

int prollyCacheInit(ProllyCache *cache, int nCapacity){
  int nBucket;

  memset(cache, 0, sizeof(*cache));
  cache->nCapacity = nCapacity;
  cache->nUsed = 0;

  nBucket = nCapacity * 2;
  if( nBucket<16 ) nBucket = 16;
  cache->nBucket = nBucket;

  cache->aBucket = (ProllyCacheEntry **)sqlite3_malloc(
    sizeof(ProllyCacheEntry *) * nBucket
  );
  if( cache->aBucket==0 ){
    return SQLITE_NOMEM;
  }
  memset(cache->aBucket, 0, sizeof(ProllyCacheEntry *) * nBucket);

  
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
      
      pEntry->nRef++;
      lruRemove(pEntry);
      lruInsertHead(cache, pEntry);
      return pEntry;
    }
    pEntry = pEntry->pHashNext;
  }

  return 0;
}

static int cacheEvictOne(ProllyCache *cache){
  ProllyCacheEntry *pEntry;

  
  pEntry = cache->lruTail.pLruPrev;
  while( pEntry!=&cache->lruHead ){
    if( pEntry->nRef==0 ){
      
      lruRemove(pEntry);
      
      hashRemove(cache, pEntry);
      
      cacheEntryFree(pEntry);
      cache->nUsed--;
      return 1;
    }
    pEntry = pEntry->pLruPrev;
  }
  return 0;
}

ProllyCacheEntry *prollyCachePut(
  ProllyCache *cache,
  const ProllyHash *hash,
  const u8 *pData,
  int nData
){
  int iBucket;
  ProllyCacheEntry *pEntry;
  u8 *pCopy;
  int rc;

  
  pEntry = prollyCacheGet(cache, hash);
  if( pEntry ){
    return pEntry;
  }

  
  while( cache->nUsed>=cache->nCapacity ){
    if( !cacheEvictOne(cache) ){
      
      break;
    }
  }

  
  pEntry = (ProllyCacheEntry *)sqlite3_malloc(sizeof(ProllyCacheEntry));
  if( pEntry==0 ) return 0;
  memset(pEntry, 0, sizeof(*pEntry));

  
  pCopy = (u8 *)sqlite3_malloc(nData);
  if( pCopy==0 ){
    sqlite3_free(pEntry);
    return 0;
  }
  memcpy(pCopy, pData, nData);

  
  memcpy(pEntry->hash.data, hash->data, PROLLY_HASH_SIZE);
  pEntry->pData = pCopy;
  pEntry->nData = nData;
  pEntry->nRef = 1;

  
  rc = prollyNodeParse(&pEntry->node, pCopy, nData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pCopy);
    sqlite3_free(pEntry);
    return 0;
  }

  
  iBucket = cacheHashBucket(cache, hash);
  pEntry->pHashNext = cache->aBucket[iBucket];
  cache->aBucket[iBucket] = pEntry;

  
  lruInsertHead(cache, pEntry);

  cache->nUsed++;
  return pEntry;
}

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry){
  (void)cache;
  assert( entry->nRef>0 );
  entry->nRef--;
}

void prollyCachePurge(ProllyCache *cache){
  ProllyCacheEntry *pEntry;
  ProllyCacheEntry *pPrev;

  if( cache->aBucket==0 ) return;

  pEntry = cache->lruTail.pLruPrev;
  while( pEntry!=&cache->lruHead ){
    pPrev = pEntry->pLruPrev;
    if( pEntry->nRef==0 ){
      lruRemove(pEntry);
      hashRemove(cache, pEntry);
      cacheEntryFree(pEntry);
      cache->nUsed--;
    }
    pEntry = pPrev;
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
