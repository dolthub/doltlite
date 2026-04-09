/* Two-tree diff: walks cursors in parallel to emit ADD/DELETE/MODIFY changes. */
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

typedef struct ProllyDiff ProllyDiff;
typedef struct ProllyDiffChange ProllyDiffChange;

struct ProllyDiffChange {
  u8 type;              
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

int diffRecordsEqualFieldwise(const u8 *pA, int nA, const u8 *pB, int nB);
int prollyValuesEqual(const u8 *pA, int nA, const u8 *pB, int nB);

/*
** Streaming diff iterator: yields one ProllyDiffChange at a time from the
** merge-walk of two prolly trees. Avoids materializing all diffs in memory.
*/
typedef struct ProllyDiffIter ProllyDiffIter;
struct ProllyDiffIter {
  ChunkStore *pStore;
  ProllyCache *pCache;
  u8 flags;

  ProllyCursor *pCurOld;
  ProllyCursor *pCurNew;

  u8 eof;             /* 1 when iteration is complete */
  int rc;             /* error code, if any */

  /* Current change (valid after a successful Step) */
  ProllyDiffChange current;

  /* Copies of value data so the change survives cursor movement */
  u8 *pOldValCopy;
  int nOldValCopy;
  u8 *pNewValCopy;
  int nNewValCopy;
};

int prollyDiffIterOpen(ProllyDiffIter *pIter, ChunkStore *pStore,
                       ProllyCache *pCache, const ProllyHash *pOldRoot,
                       const ProllyHash *pNewRoot, u8 flags);
int prollyDiffIterStep(ProllyDiffIter *pIter, ProllyDiffChange **ppChange);
void prollyDiffIterClose(ProllyDiffIter *pIter);

#endif
