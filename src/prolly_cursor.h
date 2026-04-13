
#ifndef SQLITE_PROLLY_CURSOR_H
#define SQLITE_PROLLY_CURSOR_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cache.h"
#include "chunk_store.h"

#define PROLLY_CURSOR_MAX_DEPTH 20

typedef struct ProllyCursor ProllyCursor;
typedef struct ProllyCursorLevel ProllyCursorLevel;

struct ProllyCursorLevel {
  ProllyCacheEntry *pEntry;
  int idx;
};

/* iLevel is the CURRENT depth (leaf when fully descended), nLevel is
** the depth the tree was loaded to. aLevel[0..iLevel] each hold a
** pinned ProllyCacheEntry — they must be released on cursor close or
** re-seek or the cache entries leak their node buffer. */
struct ProllyCursor {
  ChunkStore *pStore;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags;

  int nLevel;
  int iLevel;
  ProllyCursorLevel aLevel[PROLLY_CURSOR_MAX_DEPTH];

  u8 eState;

  /* Saved logical position for prollyCursorSave/Restore. After a
  ** write invalidates cache pointers, the cursor reseeks by key
  ** rather than by cached node pointers. */
  u8 *pSavedKey;
  int nSavedKey;
  i64 iSavedIntKey;
  u8 hasSavedPosition;
};

#define PROLLY_CURSOR_VALID    0
#define PROLLY_CURSOR_INVALID  1
#define PROLLY_CURSOR_EOF      2

void prollyCursorInit(ProllyCursor *cur, ChunkStore *pStore,
                      ProllyCache *pCache, const ProllyHash *pRoot, u8 flags);

int prollyCursorFirst(ProllyCursor *cur, int *pRes);

int prollyCursorLast(ProllyCursor *cur, int *pRes);

int prollyCursorNext(ProllyCursor *cur);

int prollyCursorPrev(ProllyCursor *cur);

int prollyCursorSeekInt(ProllyCursor *cur, i64 intKey, int *pRes);

int prollyCursorSeekBlob(ProllyCursor *cur,
                         const u8 *pKey, int nKey, int *pRes);

int prollyCursorIsValid(ProllyCursor *cur);

void prollyCursorKey(ProllyCursor *cur, const u8 **ppKey, int *pnKey);

i64 prollyCursorIntKey(ProllyCursor *cur);

void prollyCursorValue(ProllyCursor *cur, const u8 **ppVal, int *pnVal);

int prollyCursorSave(ProllyCursor *cur);

int prollyCursorRestore(ProllyCursor *cur, int *pDifferentRow);

void prollyCursorReleaseAll(ProllyCursor *cur);

void prollyCursorClose(ProllyCursor *cur);

#endif
