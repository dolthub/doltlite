
#ifndef SQLITE_PROLLY_DIFF_H
#define SQLITE_PROLLY_DIFF_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"

#define PROLLY_DIFF_ADD     1
#define PROLLY_DIFF_DELETE  2
#define PROLLY_DIFF_MODIFY  3

static SQLITE_INLINE const char *prollyDiffTypeName(u8 type){
  switch( type ){
    case PROLLY_DIFF_ADD:    return "added";
    case PROLLY_DIFF_DELETE: return "removed";
    case PROLLY_DIFF_MODIFY: return "modified";
  }
  return 0;
}

typedef struct ProllyDiffChange ProllyDiffChange;

struct ProllyDiffChange {
  u8 type;
  u8 keyIsIntKey;
  const u8 *pKey;
  int nKey;
  i64 intKey;
  const u8 *pOldVal;
  int nOldVal;
  const u8 *pNewVal;
  int nNewVal;
};

typedef int (*ProllyDiffCallback)(void *pCtx, const ProllyDiffChange *pChange);

int prollyDiff(ChunkStore *pStore, ProllyCache *pCache,
               const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
               u8 flags, ProllyDiffCallback xCallback, void *pCtx);

int prollyValuesEqual(
  const u8 *pA, int nA,
  const u8 *pB, int nB,
  int *pEqual
);

typedef struct ProllyDiffIter ProllyDiffIter;
struct ProllyDiffIter {
  ChunkStore *pStore;
  ProllyCache *pCache;
  u8 flags;
  u8 oldFlags;
  u8 newFlags;
  /* Different key shapes: drain old as deletes, new as adds (drop+recreate). */
  u8 shapeMismatch;

  ProllyCursor *pCurOld;
  ProllyCursor *pCurNew;

  u8 eof;
  int rc;

  ProllyDiffChange current;

  u8 *pKeyCopy;
  int nKeyCopy;

  u8 *pOldValCopy;
  int nOldValCopy;
  u8 *pNewValCopy;
  int nNewValCopy;
};

int prollyDiffIterOpen(ProllyDiffIter *pIter, ChunkStore *pStore,
                       ProllyCache *pCache,
                       const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
                       u8 oldFlags, u8 newFlags);
int prollyDiffIterStep(ProllyDiffIter *pIter, ProllyDiffChange **ppChange);
void prollyDiffIterClose(ProllyDiffIter *pIter);

/* On SQLITE_OK *ppData owns the buffer (sqlite3_free); on error nothing is allocated. */
int prollyFetchNode(ChunkStore *pStore, const ProllyHash *pHash,
                    ProllyNode *pNode, u8 **ppData);

#endif
