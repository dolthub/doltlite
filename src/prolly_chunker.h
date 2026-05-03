
#ifndef SQLITE_PROLLY_CHUNKER_H
#define SQLITE_PROLLY_CHUNKER_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "chunk_store.h"

/* MIN/MAX bracket the chunk size so degenerate inputs (adversarial
** keys, hash collisions) still produce bounded chunks. Inside that
** range, the boundary decision is made by prollyWeibullCheck which
** biases the distribution toward 4096 bytes (PROLLY_WEIBULL_L in
** prolly_hash.c). All three values are part of the on-disk content
** addressing — changing them re-hashes every tree on next write. */
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

/* Force-flush any pending content at levels < targetLevel. Each
** affected level emits a chunk (possibly below PROLLY_CHUNK_MIN —
** the boundary here is "we need to splice at targetLevel" rather
** than the natural Weibull split) and propagates its (lastKey, hash)
** to the level above.
**
** streamingMergeNode calls this before splicing a right-side sibling
** at the parent level after mergeLeaf has put content in level 0:
** without it the chunker can't accept the splice (the existing splice
** condition was `chunkerLevelsBelowEmpty`), so the loop falls through
** to recurse — walking every right-side leaf instead of an O(1)
** splice per sibling. */
int prollyChunkerDrainBelow(ProllyChunker *ch, int targetLevel);

#endif
