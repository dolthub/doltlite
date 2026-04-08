
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

struct ThreeWayChange {
  u8 type;              
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
  u8 flags,
  ThreeWayDiffCallback xCallback,
  void *pCtx
);

#endif 
