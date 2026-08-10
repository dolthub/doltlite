
#ifdef DOLTLITE_PROLLY

#include "prolly_cache.h"
#include <string.h>
#include <assert.h>

/* Trailing zero bytes appended to every cached node buffer so a reader
** parsing the last cell's record header can safely over-read one serial-type
** varint (max 9 bytes) past the payload, matching SQLite's page-buffer slack. */
#define PROLLY_NODE_BUFFER_SLOP 8

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

int prollyCacheInit(ProllyCache *cache, int nCapacity){
  int nBucket = 16;
  i64 nBucketMin = (i64)nCapacity * 2;

  memset(cache, 0, sizeof(*cache));
  if( nBucketMin>0x40000000 ) return SQLITE_NOMEM;
  cache->nCapacity = nCapacity;
  cache->nUsed = 0;

  while( nBucket<nBucketMin ) nBucket *= 2;
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

static ProllyCacheEntry *cacheEvictOne(ProllyCache *cache){
  ProllyCacheEntry *pEntry;

  pEntry = cache->lruTail.pLruPrev;
  while( pEntry!=&cache->lruHead ){
    if( pEntry->nRef==0 ){
      lruRemove(pEntry);
      hashRemove(cache, pEntry);
      sqlite3_free(pEntry->pData);
      memset(pEntry, 0, sizeof(*pEntry));
      cache->nUsed--;
      return pEntry;
    }
    pEntry = pEntry->pLruPrev;
  }
  return 0;
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
  if( cache->nUsed>=cache->nCapacity ){
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

  /* Add SQLite-style trailing slop for speculative varint reads. */
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

  iBucket = cacheHashBucket(cache, hash);
  pEntry->pHashNext = cache->aBucket[iBucket];
  cache->aBucket[iBucket] = pEntry;

  lruInsertHead(cache, pEntry);

  cache->nUsed++;
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
  (void)cache;
  assert( entry->nRef>0 );
  entry->nRef--;
  if( entry->nRef==0 && entry->bTransient ){
    cacheEntryFree(entry);
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
