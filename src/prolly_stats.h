#ifndef SQLITE_PROLLY_STATS_H
#define SQLITE_PROLLY_STATS_H

#include "sqliteInt.h"

/* Per-connection counters. A NULL pointer means the caller has no btree. */
typedef struct ProllyStats ProllyStats;
struct ProllyStats {
  u64 nCacheHit;
  u64 nCacheMiss;
  u64 nChunkRead;
  u64 nVerifyBytes;
  u64 nChunkWrite;
  u64 nHashBytes;
  u64 nSeek;
  u64 nCompare;
  u64 nSortKeyParse;
  u64 nRecordBytes;
  u64 nPendingInsert;
  u64 nPendingLookup;
  u64 nPendingMerge;
  u64 nReadAhead;
  u64 nReadAheadUnused;
};

#define prollyStatAdd(p, field, n) \
  do { if(p) (p)->field += (u64)(n); } while(0)

#endif
