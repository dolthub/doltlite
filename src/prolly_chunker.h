
#ifndef SQLITE_PROLLY_CHUNKER_H
#define SQLITE_PROLLY_CHUNKER_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"

/* May split after min if Weibull fires; must split at max. */
#define PROLLY_CHUNK_MIN     512
#define PROLLY_CHUNK_MAX     16384

typedef struct ProllyChunker ProllyChunker;
typedef struct ProllyChunkerLevel ProllyChunkerLevel;
typedef struct ProllyChunkerLeaf ProllyChunkerLeaf;

struct ProllyChunkerLevel {
  ProllyNodeBuilder builder;
  int nItems;
  int nBytes;
  u64 subtreeCount;
};

struct ProllyChunkerLeaf {
  u8 *pData;
  int nData;
  int iItem;
  u8 bCache;
};

/* Leaves whose hashes are still zero in the level-1 builder. They are hashed
** together, SIMD lanes filled across nodes, before that builder is read. */
struct ProllyChunker {
  ChunkStore *pStore;
  ProllyCache *pCache;
  u8 flags;
  int nLevels;
  ProllyChunkerLevel aLevel[PROLLY_CURSOR_MAX_DEPTH];
  ProllyHash root;
  ProllyChunkerLeaf *aLeaf;
  int nLeaf;
  int nLeafAlloc;
};

int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags);
int prollyChunkerInitWithCache(ProllyChunker *ch, ChunkStore *pStore,
                               ProllyCache *pCache, u8 flags);

int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal);

int prollyChunkerAddZeroTail(ProllyChunker *ch,
                             const u8 *pKey, int nKey,
                             const u8 *pVal, int nValPrefix,
                             i64 nZeroTail);

int prollyChunkerFinish(ProllyChunker *ch);

void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot);

void prollyChunkerFree(ProllyChunker *ch);

int prollyChunkerAddAtLevelWithCount(ProllyChunker *ch, int level,
                                     const u8 *pKey, int nKey,
                                     const u8 *pVal, int nVal,
                                     u64 subtreeCount);

int prollyChunkerAddLeafNode(ProllyChunker *ch,
                             const u8 *pKey, int nKey,
                             u8 *pData, int nData, u64 nItems);

int prollyChunkerLevelsBelowEmpty(const ProllyChunker *ch, int level);

#endif
