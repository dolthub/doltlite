
#ifndef SQLITE_PROLLY_THREE_WAY_MERGE_H
#define SQLITE_PROLLY_THREE_WAY_MERGE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cache.h"
#include "chunk_store.h"

int prollyThreeWayMergeFast(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pHandled
);

#endif
