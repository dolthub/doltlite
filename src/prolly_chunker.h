/*
** Rolling-hash tree builder for prolly trees.
** Consumes sorted key-value pairs and produces a set of content-addressed
** nodes organized into a tree structure using rolling hash boundaries.
*/
#ifndef SQLITE_PROLLY_CHUNKER_H
#define SQLITE_PROLLY_CHUNKER_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "chunk_store.h"

/*
** Target and bounds for chunk sizes.
** The rolling hash pattern determines boundaries, but we enforce
** minimum and maximum sizes to prevent degenerate cases.
*/
#define PROLLY_CHUNK_TARGET  4096
#define PROLLY_CHUNK_MIN     512
#define PROLLY_CHUNK_MAX     16384

/* Rolling hash pattern: boundary when (hash & pattern) == pattern */
#define PROLLY_CHUNK_PATTERN 0x00000FFF  /* ~1/4096 probability */

typedef struct ProllyChunker ProllyChunker;
typedef struct ProllyChunkerLevel ProllyChunkerLevel;

struct ProllyChunkerLevel {
  ProllyNodeBuilder builder;     /* Node builder for this level */
  ProllyRollingHash rh;         /* Rolling hash for boundary detection */
  int nItems;                    /* Items added at this level */
  int nBytes;                    /* Bytes accumulated at this level */
};

struct ProllyChunker {
  ChunkStore *pStore;            /* Where to write chunks */
  u8 flags;                      /* PROLLY_NODE_INTKEY or PROLLY_NODE_BLOBKEY */
  int nLevels;                   /* Number of levels currently in use */
  ProllyChunkerLevel aLevel[PROLLY_CURSOR_MAX_DEPTH];
  ProllyHash root;               /* Resulting root hash */
};

/* Initialize chunker for building a new tree */
int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags);

/* Add a leaf key-value pair. The chunker handles splitting and
** building internal nodes automatically. */
int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal);

/* Finalize the tree. Flushes all pending nodes and builds
** the root. Sets ch->root to the root hash. */
int prollyChunkerFinish(ProllyChunker *ch);

/* Get the root hash after finish */
void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot);

/* Free chunker resources */
void prollyChunkerFree(ProllyChunker *ch);

/* Add a key-value pair directly at a specific level of the chunker.
** Used by applyEdits to inject unchanged subtree hashes at the parent
** level, bypassing leaf-level re-serialization. */
int prollyChunkerAddAtLevel(ProllyChunker *ch, int level,
                            const u8 *pKey, int nKey,
                            const u8 *pVal, int nVal);

/* Flush any pending entries at the given level without finalizing.
** Used before injecting at a higher level to ensure level consistency. */
int prollyChunkerFlushLevel(ProllyChunker *ch, int level);

#endif /* SQLITE_PROLLY_CHUNKER_H */
