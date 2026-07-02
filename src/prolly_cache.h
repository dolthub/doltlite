
#ifndef SQLITE_PROLLY_CACHE_H
#define SQLITE_PROLLY_CACHE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"

typedef struct ProllyCache ProllyCache;
typedef struct ProllyCacheEntry ProllyCacheEntry;

struct ProllyCacheEntry {
  ProllyHash hash;
  u8 *pData;
  int nData;
  int nDataPhys;
  ProllyNode node;
  int nRef;
  u8 bTransient;
  ProllyCacheEntry *pLruNext;
  ProllyCacheEntry *pLruPrev;
  ProllyCacheEntry *pHashNext;
};

struct ProllyCache {
  int nCapacity;
  int nUsed;
  int nBucket;
  ProllyCacheEntry **aBucket;
  ProllyCacheEntry lruHead;
  ProllyCacheEntry lruTail;
};

int prollyCacheInit(ProllyCache *cache, int nCapacity);

ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash);

ProllyCacheEntry *prollyCachePutOwned(ProllyCache *cache,
                                      const ProllyHash *hash,
                                      u8 *pData, int nData,
                                      int *pRc);

ProllyCacheEntry *prollyCachePutTransientOwned(
                                      const ProllyHash *hash,
                                      u8 *pData, int nData, int nDataPhys,
                                      int *pRc);

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);

void prollyCacheFree(ProllyCache *cache);

#endif
