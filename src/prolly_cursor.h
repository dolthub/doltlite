/*
** Hierarchical cursor for prolly tree traversal.
** Maintains a stack of (node, index) pairs from root to leaf.
*/
#ifndef SQLITE_PROLLY_CURSOR_H
#define SQLITE_PROLLY_CURSOR_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cache.h"
#include "chunk_store.h"

/* Max tree depth (20 levels = enormous trees) */
#define PROLLY_CURSOR_MAX_DEPTH 20

typedef struct ProllyCursor ProllyCursor;
typedef struct ProllyCursorLevel ProllyCursorLevel;

struct ProllyCursorLevel {
  ProllyCacheEntry *pEntry;   /* Cached node (ref counted) */
  int idx;                    /* Current item index within node */
};

struct ProllyCursor {
  ChunkStore *pStore;         /* Chunk store for loading nodes */
  ProllyCache *pCache;        /* Node cache */
  ProllyHash root;            /* Root hash of the tree */
  u8 flags;                   /* PROLLY_NODE_INTKEY or PROLLY_NODE_BLOBKEY */

  int nLevel;                 /* Number of levels (tree height) */
  int iLevel;                 /* Current level index (0=root, nLevel-1=leaf) */
  ProllyCursorLevel aLevel[PROLLY_CURSOR_MAX_DEPTH];

  u8 eState;                  /* Cursor state: VALID, INVALID, etc. */

  /* Saved position for save/restore */
  u8 *pSavedKey;              /* Saved key for restore */
  int nSavedKey;
  i64 iSavedIntKey;           /* Saved integer key */
  u8 hasSavedPosition;        /* True if position is saved */
};

/* Cursor states */
#define PROLLY_CURSOR_VALID    0
#define PROLLY_CURSOR_INVALID  1
#define PROLLY_CURSOR_EOF      2

/* Initialize cursor for a tree */
void prollyCursorInit(ProllyCursor *cur, ChunkStore *pStore,
                      ProllyCache *pCache, const ProllyHash *pRoot, u8 flags);

/* Move to first entry. *pRes=1 if tree is empty. */
int prollyCursorFirst(ProllyCursor *cur, int *pRes);

/* Move to last entry. *pRes=1 if tree is empty. */
int prollyCursorLast(ProllyCursor *cur, int *pRes);

/* Move to next entry. Returns SQLITE_OK; sets eState=EOF at end. */
int prollyCursorNext(ProllyCursor *cur);

/* Move to previous entry. Returns SQLITE_OK; sets eState=EOF at end. */
int prollyCursorPrev(ProllyCursor *cur);

/* Seek to integer key (table btree).
** *pRes: 0=exact, -1=cursor at smaller key, +1=cursor at larger key */
int prollyCursorSeekInt(ProllyCursor *cur, i64 intKey, int *pRes);

/* Seek to blob key (index btree).
** *pRes: 0=exact, -1=cursor at smaller key, +1=cursor at larger key */
int prollyCursorSeekBlob(ProllyCursor *cur,
                         const u8 *pKey, int nKey, int *pRes);

/* Check if cursor is valid (pointing to an entry) */
int prollyCursorIsValid(ProllyCursor *cur);

/* Get current key (blob key) */
void prollyCursorKey(ProllyCursor *cur, const u8 **ppKey, int *pnKey);

/* Get current integer key */
i64 prollyCursorIntKey(ProllyCursor *cur);

/* Get current value */
void prollyCursorValue(ProllyCursor *cur, const u8 **ppVal, int *pnVal);

/* Save cursor position (for cursor save/restore) */
int prollyCursorSave(ProllyCursor *cur);

/* Restore cursor to saved position */
int prollyCursorRestore(ProllyCursor *cur, int *pDifferentRow);

/* Release all node references held by cursor */
void prollyCursorReleaseAll(ProllyCursor *cur);

/* Close cursor and free resources */
void prollyCursorClose(ProllyCursor *cur);

/* Load a node by hash from cache or chunk store.
** On success, *ppEntry is set to a cache entry with an incremented refcount.
** Used by prolly_btree.c for custom seek implementations. */
int prollyCursorLoadNode(ProllyCursor *cur, const ProllyHash *hash,
                         ProllyCacheEntry **ppEntry);

#endif /* SQLITE_PROLLY_CURSOR_H */
