/*
** Three-way row diff engine for prolly trees.
**
** Given an ancestor tree, "ours" tree, and "theirs" tree, computes
** a merged stream of changes by running two prollyDiff passes
** (ancestor→ours, ancestor→theirs) and merge-walking both result
** streams in sorted key order.
**
** Change types:
**   LEFT_ADD/DELETE/MODIFY   — change only in "ours"
**   RIGHT_ADD/DELETE/MODIFY  — change only in "theirs"
**   CONVERGENT               — both sides made the same change
**   CONFLICT_MM              — both modified, but to different values
**   CONFLICT_DM              — one deleted, the other modified
*/
#ifndef SQLITE_PROLLY_THREE_WAY_DIFF_H
#define SQLITE_PROLLY_THREE_WAY_DIFF_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_diff.h"

/* Three-way diff change types */
#define THREE_WAY_LEFT_ADD       1
#define THREE_WAY_LEFT_DELETE    2
#define THREE_WAY_LEFT_MODIFY    3
#define THREE_WAY_RIGHT_ADD      4
#define THREE_WAY_RIGHT_DELETE   5
#define THREE_WAY_RIGHT_MODIFY   6
#define THREE_WAY_CONVERGENT     7
#define THREE_WAY_CONFLICT_MM    8   /* Both modified differently */
#define THREE_WAY_CONFLICT_DM    9   /* One deleted, other modified */

typedef struct ThreeWayChange ThreeWayChange;

struct ThreeWayChange {
  u8 type;              /* THREE_WAY_* constant */
  const u8 *pKey;       /* Key data (NULL for INTKEY) */
  int nKey;             /* Key size */
  i64 intKey;           /* Integer key (if INTKEY) */
  const u8 *pBaseVal;   /* Ancestor value (NULL for ADDs) */
  int nBaseVal;
  const u8 *pOurVal;    /* Our value (NULL for DELETEs on our side) */
  int nOurVal;
  const u8 *pTheirVal;  /* Their value (NULL for DELETEs on their side) */
  int nTheirVal;
};

/* Callback for each three-way difference found */
typedef int (*ThreeWayDiffCallback)(void *pCtx, const ThreeWayChange *pChange);

/*
** Compute three-way diff between ancestor, ours, and theirs trees.
** Calls xCallback for each difference found.
*/
int prollyThreeWayDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncestorRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ThreeWayDiffCallback xCallback,
  void *pCtx
);

#endif /* SQLITE_PROLLY_THREE_WAY_DIFF_H */
