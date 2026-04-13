/* Generic chunk graph classification/walk helpers shared by prolly and DoltLite. */
#ifndef SQLITE_PROLLY_CHUNK_WALK_H
#define SQLITE_PROLLY_CHUNK_WALK_H

#include "sqliteInt.h"
#include "prolly_hash.h"

typedef enum {
  CHUNK_UNKNOWN = 0,
  CHUNK_COMMIT,
  CHUNK_PROLLY_NODE,
  CHUNK_CATALOG,
  CHUNK_WORKING_SET,
  CHUNK_REFS
} DoltliteChunkType;

typedef int (*DoltliteChildCb)(void *ctx, const ProllyHash *pHash);

DoltliteChunkType doltliteClassifyChunk(const u8 *data, int nData);

int doltliteEnumerateChunkChildren(
  const u8 *data,
  int nData,
  DoltliteChildCb xChild,
  void *ctx
);

#endif
