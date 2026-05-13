
#ifdef DOLTLITE_PROLLY

#include "chunk_index.h"
#include <string.h>

void chunkIndexInit(ChunkIndex *idx){
  memset(idx, 0, sizeof(*idx));
}

void chunkIndexReset(ChunkIndex *idx){
  memset(idx, 0, sizeof(*idx));
}

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par){
  *pn = idx->nIndex;
  *par = idx->aIndex;
}

int chunkIndexCount(const ChunkIndex *idx){
  return idx->nIndex;
}

int chunkIndexNChunks(const ChunkIndex *idx){
  return idx->nChunks;
}

i64 chunkIndexOffset(const ChunkIndex *idx){
  return idx->iIndexOffset;
}

i64 chunkIndexSize(const ChunkIndex *idx){
  return idx->nIndexSize;
}

void chunkIndexSetMetadata(ChunkIndex *idx, int nChunks, i64 iOffset, i64 nSize){
  idx->nChunks = nChunks;
  idx->iIndexOffset = iOffset;
  idx->nIndexSize = nSize;
}

void chunkIndexReplaceEntries(ChunkIndex *idx, ChunkIndexEntry *aNew, int nNew){
  sqlite3_free(idx->aIndex);
  idx->aIndex = aNew;
  idx->nIndex = nNew;
  idx->aIndexMmapBase = 0;
  idx->aIndexMmapSize = 0;
}

#endif
