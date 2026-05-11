
#ifndef SQLITE_PROLLY_CHUNKER_H
#define SQLITE_PROLLY_CHUNKER_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "chunk_store.h"

/* Min/max bounds for content-defined chunks. The chunker may split after the
** minimum when the key hash passes prollyWeibullCheck, and must split at max. */
#define PROLLY_CHUNK_MIN     512
#define PROLLY_CHUNK_MAX     16384

typedef struct ProllyChunker ProllyChunker;
typedef struct ProllyChunkerLevel ProllyChunkerLevel;

struct ProllyChunkerLevel {
  ProllyNodeBuilder builder;
  int nItems;
  int nBytes;
};

struct ProllyChunker {
  ChunkStore *pStore;
  u8 flags;
  int nLevels;
  ProllyChunkerLevel aLevel[PROLLY_CURSOR_MAX_DEPTH];
  ProllyHash root;
};

int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags);

int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal);

int prollyChunkerFinish(ProllyChunker *ch);

void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot);

void prollyChunkerFree(ProllyChunker *ch);

int prollyChunkerAddAtLevel(ProllyChunker *ch, int level,
                            const u8 *pKey, int nKey,
                            const u8 *pVal, int nVal);

#endif
