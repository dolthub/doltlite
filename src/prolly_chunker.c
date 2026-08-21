
#ifdef DOLTLITE_PROLLY

#include "prolly_chunker.h"
#include "prolly_cursor.h"
#include "prolly_xxhash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int initLevel(ProllyChunker *ch, int level);
static int flushLevel(ProllyChunker *ch, int level);
static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal, i64 nZeroTail,
                      u64 childSubtreeCount);
static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash);
static int hasPendingAncestorLevels(const ProllyChunker *ch, int level);
static int finishPropagateLevel(ProllyChunker *ch, int level);

static int initLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );

  pLevel = &ch->aLevel[level];
  memset(pLevel, 0, sizeof(ProllyChunkerLevel));

  prollyNodeBuilderInit(&pLevel->builder, (u8)level, ch->flags);

  pLevel->nItems = 0;
  pLevel->nBytes = 0;
  pLevel->subtreeCount = 0;

  return SQLITE_OK;
}

static void builderLastKey(ProllyNodeBuilder *b,
                           const u8 **ppKey, int *pnKey){
  int n = b->nItems;
  u32 off0, off1;
  assert( n > 0 );
  off0 = b->aKeyOff[n - 1];
  off1 = b->aKeyOff[n];
  *ppKey = b->pKeyBuf + off0;
  *pnKey = (int)(off1 - off0);
}

static void resetLevel(ProllyChunkerLevel *pLevel){
  prollyNodeBuilderReset(&pLevel->builder);
  pLevel->nItems = 0;
  pLevel->nBytes = 0;
  pLevel->subtreeCount = 0;
}

static int flushLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  ProllyHash hash;
  const u8 *pLastKey;
  int nLastKey;
  u64 flushedCount;
  int rc;

  builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);
  flushedCount = pLevel->subtreeCount;

  rc = finishFlushLevel(ch, level, &hash);
  if( rc!=SQLITE_OK ) return rc;

  if( level + 1 >= PROLLY_CURSOR_MAX_DEPTH ){
    return SQLITE_FULL;
  }

  rc = addToLevel(ch, level + 1,
                  pLastKey, nLastKey,
                  hash.data, PROLLY_HASH_SIZE, 0,
                  flushedCount);
  if( rc!=SQLITE_OK ) return rc;

  resetLevel(pLevel);
  return SQLITE_OK;
}

static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  i64 nZeroTail = 0;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  rc = prollyNodeBuilderFinishSparse(&pLevel->builder, &pData, &nData,
                                     &nZeroTail);
  if( rc!=SQLITE_OK ) return rc;

  if( nZeroTail>0 ){
    /* Sparse leaves skip the cache; caching would materialize the tail. */
    rc = chunkStorePutSparse(ch->pStore, pData, nData - (int)nZeroTail,
                             nZeroTail, pHash);
    sqlite3_free(pData);
    return rc;
  }

  rc = chunkStorePut(ch->pStore, pData, nData, pHash);
  if( rc==SQLITE_OK && ch->pCache ){
    ProllyCacheEntry *pEntry;
    pEntry = prollyCachePutOwned(ch->pCache, pHash, pData, nData, &rc);
    pData = 0;  /* consumed on every PutOwned path, including failure */
    if( pEntry ) prollyCacheRelease(ch->pCache, pEntry);
  }
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

static int hasPendingAncestorLevels(const ProllyChunker *ch, int level){
  int k;
  for( k = level + 1; k < ch->nLevels; k++ ){
    if( ch->aLevel[k].builder.nItems > 0 ){
      return 1;
    }
  }
  return 0;
}

static int finishPropagateLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  ProllyHash hash;
  const u8 *pLastKey;
  int nLastKey;
  u64 flushedCount;
  int rc;

  builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);
  flushedCount = pLevel->subtreeCount;
  rc = finishFlushLevel(ch, level, &hash);
  if( rc!=SQLITE_OK ) return rc;

  rc = addToLevel(ch, level + 1, pLastKey, nLastKey,
                  hash.data, PROLLY_HASH_SIZE, 0, flushedCount);
  if( rc!=SQLITE_OK ) return rc;

  resetLevel(pLevel);
  return SQLITE_OK;
}

static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal, i64 nZeroTail,
                      u64 childSubtreeCount){
  ProllyChunkerLevel *pLevel;
  int rc;
  int thisSize;
  u64 incCount;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );
  assert( nZeroTail==0 || level==0 );

  if( level >= ch->nLevels ){
    while( ch->nLevels <= level ){
      rc = initLevel(ch, ch->nLevels);
      if( rc!=SQLITE_OK ) return rc;
      ch->nLevels++;
    }
  }

  pLevel = &ch->aLevel[level];

  if( level==0 ){
    incCount = 1;
    if( nZeroTail>0 ){
      rc = prollyNodeBuilderAddZeroTail(&pLevel->builder, pKey, nKey,
                                        pVal, nVal, nZeroTail);
      nVal += (int)nZeroTail;  /* size accounting below is logical */
    }else{
      rc = prollyNodeBuilderAdd(&pLevel->builder, pKey, nKey, pVal, nVal);
    }
  }else{
    incCount = childSubtreeCount;
    rc = prollyNodeBuilderAddWithCount(&pLevel->builder, pKey, nKey,
                                       pVal, nVal, childSubtreeCount);
  }
  if( rc!=SQLITE_OK ) return rc;

  pLevel->nItems++;
  pLevel->subtreeCount += incCount;

  thisSize = PROLLY_NODE_ENTRY_BYTES(level, nKey, nVal);
  pLevel->nBytes += thisSize;

  /* One oversize entry cannot split; flushing it would propagate SQLITE_FULL. */
  if( pLevel->nItems>1 && pLevel->nBytes >= PROLLY_CHUNK_MIN ){
    int atBoundary;
    if( pLevel->nBytes >= PROLLY_CHUNK_MAX ){
      atBoundary = 1;
    } else {
      u32 h = prollyXXH32(pKey, nKey, (u32)level);
      atBoundary = prollyWeibullCheck((u32)pLevel->nBytes,
                                       (u32)thisSize, h);
    }
    if( atBoundary ){
      rc = flushLevel(ch, level);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags){
  return prollyChunkerInitWithCache(ch, pStore, 0, flags);
}

int prollyChunkerInitWithCache(ProllyChunker *ch, ChunkStore *pStore,
                               ProllyCache *pCache, u8 flags){
  memset(ch, 0, sizeof(ProllyChunker));
  ch->pStore = pStore;
  ch->pCache = pCache;
  ch->flags = flags;
  ch->nLevels = 0;
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal){
  int rc;

  rc = addToLevel(ch, 0, pKey, nKey, pVal, nVal, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

int prollyChunkerAddZeroTail(ProllyChunker *ch,
                             const u8 *pKey, int nKey,
                             const u8 *pVal, int nValPrefix,
                             i64 nZeroTail){
  return addToLevel(ch, 0, pKey, nKey, pVal, nValPrefix, nZeroTail, 0);
}

int prollyChunkerFinish(ProllyChunker *ch){
  int rc;
  int level;

  if( ch->nLevels == 0 ){
    memset(&ch->root, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  level = 0;
  while( level < ch->nLevels ){
    ProllyChunkerLevel *pLevel = &ch->aLevel[level];

    if( pLevel->builder.nItems == 0 ){

      level++;
      continue;
    }

    {
      if( hasPendingAncestorLevels(ch, level) ){
        rc = finishPropagateLevel(ch, level);
        if( rc!=SQLITE_OK ) return rc;
      } else {

        ProllyHash hash;

        rc = finishFlushLevel(ch, level, &hash);
        if( rc!=SQLITE_OK ) return rc;

        memcpy(&ch->root, &hash, sizeof(ProllyHash));

        prollyNodeBuilderReset(&pLevel->builder);
        pLevel->nItems = 0;
        pLevel->nBytes = 0;
        return SQLITE_OK;
      }
    }

    level++;
  }

#ifdef DOLTLITE_PROLLY_CHECK
  if( ch->nLevels > 0 ){
    fprintf(stderr,
            "doltlite: S5 invariant: prollyChunkerFinish has nLevels=%d "
            "but every level is empty; would have returned empty root\n",
            ch->nLevels);
    abort();
  }
#endif
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot){
  memcpy(pRoot, &ch->root, sizeof(ProllyHash));
}

int prollyChunkerAddAtLevelWithCount(ProllyChunker *ch, int level,
                                     const u8 *pKey, int nKey,
                                     const u8 *pVal, int nVal,
                                     u64 subtreeCount){
  return addToLevel(ch, level, pKey, nKey, pVal, nVal, 0, subtreeCount);
}

void prollyChunkerFree(ProllyChunker *ch){
  int i;
  for(i = 0; i < ch->nLevels; i++){
    prollyNodeBuilderFree(&ch->aLevel[i].builder);
  }
  ch->nLevels = 0;
}

int prollyChunkerLevelsBelowEmpty(const ProllyChunker *ch, int level){
  int i;
  for(i = 0; i < level && i < ch->nLevels; i++){
    if( ch->aLevel[i].builder.nItems > 0 ) return 0;
  }
  return 1;
}

#endif
