
#ifndef SQLITE_PROLLY_MUTATE_H
#define SQLITE_PROLLY_MUTATE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_mutmap.h"
#include "prolly_chunker.h"
#include "chunk_store.h"
#include "prolly_cache.h"

typedef struct ProllyMutator ProllyMutator;

struct ProllyMutator {
  ChunkStore *pStore;
  ProllyCache *pCache;
  ProllyHash oldRoot;
  ProllyMutMap *pEdits;
  u8 flags;
  ProllyHash newRoot;
};

int prollyMutateFlush(ProllyMutator *pMut);

int prollyMutateInsert(ChunkStore *pStore, ProllyCache *pCache,
                       const ProllyHash *pRoot, u8 flags,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal,
                       ProllyHash *pNewRoot);

int prollyMutateDelete(ChunkStore *pStore, ProllyCache *pCache,
                       const ProllyHash *pRoot, u8 flags,
                       const u8 *pKey, int nKey, i64 intKey,
                       ProllyHash *pNewRoot);

#endif
