
#ifndef DOLTLITE_CHUNK_INDEX_H
#define DOLTLITE_CHUNK_INDEX_H

#include "sqliteInt.h"
#include "prolly_hash.h"

typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ChunkIndex ChunkIndex;

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

struct ChunkIndex {
  ChunkIndexEntry *aIndex;
  int nIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  int nChunks;
  i64 iIndexOffset;
  i64 nIndexSize;
};

void chunkIndexInit(ChunkIndex *idx);
void chunkIndexReset(ChunkIndex *idx);

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par);
int chunkIndexCount(const ChunkIndex *idx);
int chunkIndexNChunks(const ChunkIndex *idx);
i64 chunkIndexOffset(const ChunkIndex *idx);
i64 chunkIndexSize(const ChunkIndex *idx);

void chunkIndexSetMetadata(ChunkIndex *idx, int nChunks, i64 iOffset, i64 nSize);
void chunkIndexReplaceEntries(ChunkIndex *idx, ChunkIndexEntry *aNew, int nNew);

struct ChunkStore;
int csReadIndex(struct ChunkStore *cs);
int csSearchIndex(const ChunkIndexEntry *aIdx, int nIdx, const ProllyHash *pHash);
int csMergeIndex(struct ChunkStore *cs, ChunkIndexEntry **ppMerged, int *pnMerged);
void csReleaseIndexBuf(ChunkIndexEntry *aIndex, void *mmapBase, i64 mmapSize);

#endif
