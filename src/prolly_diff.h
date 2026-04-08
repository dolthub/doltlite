
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

#endif 
