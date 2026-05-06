
#ifndef SQLITE_PROLLY_CHUNKER_H
#define SQLITE_PROLLY_CHUNKER_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "chunk_store.h"

/* Target is the expected chunk size; MIN/MAX bracket it so a
** degenerate rolling hash (long runs of zero bytes, repeated keys)
** still produces bounded chunks. PATTERN is the bitmask the rolling
** hash is tested against: on average 1/(2^bits) positions match, so
** 12 zero bits → ~4096 expected chunk size. Changing PATTERN here
** re-hashes every tree on next write — it's part of the on-disk
** content-addressing, not a tuning knob. */
#define PROLLY_CHUNK_TARGET  4096
#define PROLLY_CHUNK_MIN     512
#define PROLLY_CHUNK_MAX     16384

#define PROLLY_CHUNK_PATTERN 0x00000FFF

typedef struct ProllyChunker ProllyChunker;
typedef struct ProllyChunkerLevel ProllyChunkerLevel;

struct ProllyChunkerLevel {
  ProllyNodeBuilder builder;
  ProllyRollingHash rh;
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

int prollyChunkerFlushLevel(ProllyChunker *ch, int level);

#endif
