
#ifndef SQLITE_PROLLY_THREE_WAY_DIFF_H
#define SQLITE_PROLLY_THREE_WAY_DIFF_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_diff.h"

#define THREE_WAY_LEFT_ADD       1
#define THREE_WAY_LEFT_DELETE    2
#define THREE_WAY_LEFT_MODIFY    3
#define THREE_WAY_RIGHT_ADD      4
#define THREE_WAY_RIGHT_DELETE   5
#define THREE_WAY_RIGHT_MODIFY   6
#define THREE_WAY_CONVERGENT     7
#define THREE_WAY_CONFLICT_MM    8
#define THREE_WAY_CONFLICT_DM    9

typedef struct ThreeWayChange ThreeWayChange;

#define THREE_WAY_HAS_BASE   0x01
#define THREE_WAY_HAS_OURS   0x02
#define THREE_WAY_HAS_THEIRS 0x04

struct ThreeWayChange {
  u8 type;
  /* Which sides hold the row. A row stored with no value has a zero
  ** length, so the lengths alone cannot say. */
  u8 mPresent;
  const u8 *pKey;
  int nKey;
  i64 intKey;
  const u8 *pBaseVal;
  int nBaseVal;
  const u8 *pOurVal;
  int nOurVal;
  const u8 *pTheirVal;
  int nTheirVal;
};

typedef int (*ThreeWayDiffCallback)(void *pCtx, const ThreeWayChange *pChange);

int prollyThreeWayDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncestorRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 ancFlags,
  u8 oursFlags,
  u8 theirsFlags,
  ThreeWayDiffCallback xCallback,
  void *pCtx
);

#endif
