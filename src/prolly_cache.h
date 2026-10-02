
#ifndef SQLITE_PROLLY_CACHE_H
#define SQLITE_PROLLY_CACHE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_stats.h"

#define PROLLY_CACHE_SHARED_PREFIX 32
/* Bit 0 is cleared by a point read. Bit 1 stays set when a write scan
** loaded the leaf, so that scan can still drop a text key from the prefix. */
#define PROLLY_CACHE_SCAN_ONLY 0x01
#define PROLLY_CACHE_SCAN_KEEP 0x02
#define PROLLY_CACHE_SCAN_DROP 0x04

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
  u8 bReadAheadUnused;
  u8 bWideFull;
  u8 bWideRow;
  void *pBorrow;
  u64 iTouch;
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
  u64 iClock;
  i64 nWideFull;
  u32 nReadAheadStarved;
  ProllyCacheEntry **aBucket;
  ProllyCacheEntry lruHead;
  ProllyCacheEntry lruTail;
  ProllyCacheEntry prefixHead;
  ProllyCacheEntry prefixTail;
  ProllyCacheEntry wideHead;
  ProllyCacheEntry wideTail;
  ProllyCacheEntry rowHead;
  ProllyCacheEntry rowTail;
  ProllyStats *pStats;
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

/* Caches a committed chunk the store lends in place (chunkStoreBorrow). The
** entry holds the loan and counts only its own bytes against the budget. */
ProllyCacheEntry *prollyCachePutBorrowed(ProllyCache *cache,
                                         const ProllyHash *hash,
                                         const u8 *pData, int nData,
                                         void *pBorrow, int *pRc);
ProllyCacheEntry *prollyCachePutTransientOwned(
                                      const ProllyHash *hash,
                                      u8 *pData, int nData, int nDataPhys,
                                      int *pRc);

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);
void prollyCacheReleaseScan(ProllyCache *cache, ProllyCacheEntry *entry);
i64 prollyCacheCompactBytes(const ProllyNode *pNode);

void prollyCacheFree(ProllyCache *cache);

/* Rebuild the record prefix for an elided cache entry into pOut.
** *pnAvail is the number of original record bytes available. */
int prollyCacheExpandElidedPrefix(
  const ProllyNode *pNode, int iItem,
  u8 *pOut, int nOutCap, int *pnAvail, ProllyStats *pStats);

#endif
