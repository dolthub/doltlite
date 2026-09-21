
#ifndef SQLITE_PROLLY_CACHE_H
#define SQLITE_PROLLY_CACHE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "chunk_store.h"

/* Trailing zeros so parsing the last cell can over-read one varint (max 9 bytes). */
#define PROLLY_NODE_BUFFER_SLOP 8

typedef struct ProllyCache ProllyCache;
typedef struct ProllyCacheEntry ProllyCacheEntry;

struct ProllyCacheEntry {
  ProllyHash hash;
  u8 *pAlloc;
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
  i64 nMaxByte;
  i64 nByte;
  int nUsed;
  int nBucket;
  ProllyCacheEntry **aBucket;
  ProllyCacheEntry lruHead;
  ProllyCacheEntry lruTail;
};

int prollyCacheInit(ProllyCache *cache, i64 nMaxByte);

void prollyCacheSetBudget(ProllyCache *cache, i64 nMaxByte);

ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash);

ProllyCacheEntry *prollyCachePutOwned(ProllyCache *cache,
                                      const ProllyHash *hash,
                                      u8 *pData, int nData,
                                      int *pRc);

ProllyCacheEntry *prollyCachePutBufferOwned(ProllyCache *cache,
                                           const ProllyHash *hash,
                                           ChunkBuffer *pBuffer, int *pRc);

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);

void prollyCacheFree(ProllyCache *cache);

#endif
