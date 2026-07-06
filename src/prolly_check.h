
#ifndef SQLITE_PROLLY_CHECK_H
#define SQLITE_PROLLY_CHECK_H

#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#ifdef DOLTLITE_PROLLY_CHECK

int prollyCheckTree(
  ChunkStore *pStore,
  const ProllyHash *pRoot,
  u8 flags,
  char **pzErr
);

int prollyCheckNodeChildrenPresent(
  ChunkStore *pStore,
  const u8 *pData,
  int nData,
  const char *zContext
);

#else

static SQLITE_INLINE int prollyCheckTree(
  ChunkStore *pStore,
  const ProllyHash *pRoot,
  u8 flags,
  char **pzErr
){
  (void)pStore; (void)pRoot; (void)flags;
  if( pzErr ) *pzErr = 0;
  return SQLITE_OK;
}

static SQLITE_INLINE int prollyCheckNodeChildrenPresent(
  ChunkStore *pStore,
  const u8 *pData,
  int nData,
  const char *zContext
){
  (void)pStore; (void)pData; (void)nData; (void)zContext;
  return SQLITE_OK;
}

#endif

#endif
#endif
