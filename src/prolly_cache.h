
#ifndef SQLITE_PROLLY_CACHE_H
#define SQLITE_PROLLY_CACHE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"

#define PROLLY_CACHE_SHARED_PREFIX 32

typedef struct ProllyCache ProllyCache;
typedef struct ProllyCacheEntry ProllyCacheEntry;

struct ProllyCacheEntry {
  ProllyHash hash;
  u8 *pData;
  u8 *pPacked;
  ProllyNode node;
  int nRef;
  u8 bTransient;
  u8 nEvictChance;
  u8 bScanOnly;
  u8 bAllowPrefix;
  ProllyCacheEntry *pLruNext;
  ProllyCacheEntry *pLruPrev;
  ProllyCacheEntry *pHashNext;
};

struct ProllyCache {
  i64 nMaxByte;
  i64 nByte;
  int nUsed;
  int nSharedPrefix;
  int nBucket;
  ProllyCacheEntry **aBucket;
  ProllyCacheEntry lruHead;
  ProllyCacheEntry lruTail;
  ProllyCacheEntry prefixHead;
  ProllyCacheEntry prefixTail;
};

int prollyCacheInit(ProllyCache *cache, i64 nMaxByte);

void prollyCacheSetBudget(ProllyCache *cache, i64 nMaxByte);
void prollyCacheShrink(ProllyCache *cache);

ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash);
ProllyCacheEntry *prollyCacheGetPrefix(ProllyCache*, const ProllyHash*, int);
ProllyCacheEntry *prollyCacheGetForScan(ProllyCache *cache,
                                      const ProllyHash *hash);

ProllyCacheEntry *prollyCachePutOwned(ProllyCache *cache,
                                      const ProllyHash *hash,
                                      u8 *pData, int nData,
                                      int *pRc);

ProllyCacheEntry *prollyCachePutTransientOwned(
                                      const ProllyHash *hash,
                                      u8 *pData, int nData, int nDataPhys,
                                      int *pRc);

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);
void prollyCacheReleaseScan(ProllyCache *cache, ProllyCacheEntry *entry);

void prollyCacheFree(ProllyCache *cache);

/* Rebuild the record prefix for an elided cache entry into pOut.
** *pnAvail is the number of original record bytes available. */
int prollyCacheExpandElidedPrefix(
  const ProllyNode *pNode, int iItem,
  u8 *pOut, int nOutCap, int *pnAvail);

#endif
