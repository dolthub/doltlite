
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
  ProllyNode node;           
  int nRef;                  
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

ProllyCacheEntry *prollyCachePut(ProllyCache *cache,
                                  const ProllyHash *hash,
                                  const u8 *pData, int nData);

void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);

void prollyCachePurge(ProllyCache *cache);

void prollyCacheFree(ProllyCache *cache);

#endif 
