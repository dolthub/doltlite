
#ifndef DOLTLITE_CHUNK_INDEX_H
#define DOLTLITE_CHUNK_INDEX_H

#include "sqliteInt.h"
#include "prolly_hash.h"

typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ChunkIndex ChunkIndex;
typedef struct ChunkIndexLazy ChunkIndexLazy;
typedef struct ChunkIndexCache ChunkIndexCache;

#if defined(__GNUC__) || defined(__clang__)
#  define DOLTLITE_PACKED __attribute__((__packed__))
#elif defined(_MSC_VER)
#  define DOLTLITE_PACKED
#  pragma pack(push, 1)
#else
#  define DOLTLITE_PACKED
#endif

struct DOLTLITE_PACKED ChunkIndexEntry {
  ProllyHash hash;
  i64 offset;
  int size;
};

#if defined(_MSC_VER)
#  pragma pack(pop)
#endif

struct ChunkIndexLazy {
  i64 iRootOffset;
  i64 iDataEnd;
  int nRootSize;
  int nEntries;
  ProllyHash rootHash;
  u8 active;
};

struct ChunkIndex {
  ChunkIndexEntry *aIndex;
  int nIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  int nChunks;
  i64 iIndexOffset;
  i64 nIndexSize;
  ChunkIndexLazy lazy;
};

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par);
int chunkIndexCount(const ChunkIndex *idx);

void chunkIndexSetMetadata(ChunkIndex *idx, int nChunks, i64 iOffset, i64 nSize);
void chunkIndexReplaceEntries(ChunkIndex *idx, ChunkIndexEntry *aNew, int nNew);

struct ChunkStore;
typedef struct ChunkIndexSpool ChunkIndexSpool;
typedef int (*CsIndexVisitor)(void*, const ChunkIndexEntry*);
int csVisitIndex(struct ChunkStore*, CsIndexVisitor, void*);
int csIndexSpoolInit(sqlite3_vfs*, ChunkIndexSpool**);
void csIndexSpoolFree(ChunkIndexSpool*);
int csIndexSpoolAdd(void*, const ChunkIndexEntry*);
int csIndexSpoolFinish(ChunkIndexSpool*);
int csIndexSpoolCount(const ChunkIndexSpool*);
int csIndexSpoolGet(ChunkIndexSpool*, int, ChunkIndexEntry*);
int csIndexSpoolFind(ChunkIndexSpool*, const ProllyHash*, int*);
int csIndexSnapshot(struct ChunkStore*, int, ChunkIndexSpool**);
void csIndexInstallFlat(ChunkIndex*);
int csReadIndex(struct ChunkStore *cs);
int csIndexLookup(struct ChunkStore *cs, const ProllyHash *pHash,
                  ChunkIndexEntry *pEntry, int *pFound);
void csIndexCacheFree(struct ChunkStore *cs);
i64 csIndexCacheSetBudget(struct ChunkStore *cs, i64 nByte);
i64 csIndexCacheBytes(const struct ChunkStore *cs);
int csMaterializeIndex(struct ChunkStore *cs);
int csSearchIndex(const ChunkIndexEntry *aIdx, int nIdx, const ProllyHash *pHash);
int csIndexEntryCmp(const void *a, const void *b);
int csMergeIndex(struct ChunkStore *cs, ChunkIndexEntry **ppMerged, int *pnMerged);
void csReleaseIndexBuf(ChunkIndexEntry *aIndex, void *mmapBase, i64 mmapSize);

#endif
