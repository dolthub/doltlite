
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
static int finishFlushLevel(ProllyChunker *ch, int level, int bDefer,
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

  rc = finishFlushLevel(ch, level, 1, &hash);
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

static int deferLeaf(ProllyChunker *ch, u8 *pData, int nData, u8 bCache){
  ProllyChunkerLeaf *p;
  if( ch->nLeaf==ch->nLeafAlloc ){
    int nNew = ch->nLeafAlloc ? ch->nLeafAlloc*2 : 64;
    ProllyChunkerLeaf *aNew;
    sqlite3BeginBenignMalloc();
    aNew = sqlite3_realloc64(ch->aLeaf, (u64)nNew*sizeof(ProllyChunkerLeaf));
    sqlite3EndBenignMalloc();
    if( !aNew ) return SQLITE_NOMEM;
    ch->aLeaf = aNew;
    ch->nLeafAlloc = nNew;
  }
  p = &ch->aLeaf[ch->nLeaf++];
  p->pData = pData;
  p->nData = nData;
  p->iItem = ch->nLevels>1 ? ch->aLevel[1].builder.nItems : 0;
  p->bCache = bCache;
  return SQLITE_OK;
}

static int resolveLeaves(ProllyChunker *ch){
  ProllyNodeBuilder *b = &ch->aLevel[1].builder;
  int rc = SQLITE_OK;
  int i, j;
  for(i=0; i<ch->nLeaf && rc==SQLITE_OK; i+=16){
    const u8 *apData[16];
    int anData[16];
    ProllyHash aHash[16];
    int n = ch->nLeaf-i < 16 ? ch->nLeaf-i : 16;
    for(j=0; j<n; j++){
      apData[j] = ch->aLeaf[i+j].pData;
      anData[j] = ch->aLeaf[i+j].nData;
    }
    prollyHashComputeMany(apData, anData, n, aHash);
    for(j=0; j<n && rc==SQLITE_OK; j++){
      ProllyChunkerLeaf *p = &ch->aLeaf[i+j];
      assert( p->iItem<b->nItems );
      rc = chunkStorePutHashed(ch->pStore, p->pData, p->nData, &aHash[j]);
      if( rc!=SQLITE_OK ) break;
      memcpy(b->pValBuf + b->aValOff[p->iItem], aHash[j].data,
             PROLLY_HASH_SIZE);
      if( p->bCache && ch->pCache ){
        ProllyCacheEntry *pEntry;
        pEntry = prollyCachePutOwned(ch->pCache, &aHash[j], p->pData,
                                     p->nData, &rc);
        p->pData = 0;
        if( pEntry ) prollyCacheRelease(ch->pCache, pEntry);
      }
    }
  }
  for(i=0; i<ch->nLeaf; i++) sqlite3_free(ch->aLeaf[i].pData);
  ch->nLeaf = 0;
  return rc;
}

static int finishFlushLevel(ProllyChunker *ch, int level, int bDefer,
                            ProllyHash *pHash){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  i64 nZeroTail = 0;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  if( level==1 && ch->nLeaf ){
    rc = resolveLeaves(ch);
    if( rc!=SQLITE_OK ) return rc;
  }

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

  if( level==0 && bDefer
   && deferLeaf(ch, pData, nData, 1)==SQLITE_OK ){
    memset(pHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
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

static int chunkerPeekNode(ProllyChunker *ch, const ProllyHash *pHash,
                           int *pLevel, int *pnItems, ProllyHash *pChild){
  ProllyCacheEntry *pEntry = ch->pCache ? prollyCacheGet(ch->pCache, pHash) : 0;
  ProllyNode node;
  u8 *pData = 0;
  int nData = 0;
  int nDataPhys = 0;
  int rc = SQLITE_OK;
  if( pEntry ){
    *pLevel = pEntry->node.level;
    *pnItems = pEntry->node.nItems;
    if( *pLevel>0 && *pnItems>0 ) prollyNodeChildHash(&pEntry->node, 0, pChild);
    prollyCacheRelease(ch->pCache, pEntry);
    return SQLITE_OK;
  }
  rc = chunkStoreGetSparse(ch->pStore, pHash, &pData, &nData, &nDataPhys);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParseSparse(&node, pData, nData, nDataPhys);
  if( rc==SQLITE_OK ){
    *pLevel = node.level;
    *pnItems = node.nItems;
    if( *pLevel>0 && *pnItems>0 ) prollyNodeChildHash(&node, 0, pChild);
  }
  sqlite3_free(pData);
  return rc;
}

static int canonicalRootFromSingleChild(ProllyChunker *ch,
                                        ProllyNodeBuilder *b,
                                        ProllyHash *pRoot){
  ProllyHash cur;
  int depth;
  assert( b->level>0 && b->nItems==1 );
  if( b->level==1 && ch->nLeaf ){
    int rc = resolveLeaves(ch);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( (int)(b->aValOff[1] - b->aValOff[0])!=PROLLY_HASH_SIZE ){
    return SQLITE_CORRUPT;
  }
  memcpy(cur.data, b->pValBuf + b->aValOff[0], PROLLY_HASH_SIZE);
  for(depth = 0; depth < PROLLY_CURSOR_MAX_DEPTH; depth++){
    int level = 0, nItems = 0, rc;
    ProllyHash child;
    rc = chunkerPeekNode(ch, &cur, &level, &nItems, &child);
    if( rc!=SQLITE_OK ) return rc;
    if( level==0 || nItems>1 ){
      memcpy(pRoot, &cur, sizeof(ProllyHash));
      return SQLITE_OK;
    }
    if( nItems==0 ) return SQLITE_CORRUPT;
    memcpy(&cur, &child, sizeof(ProllyHash));
  }
  return SQLITE_CORRUPT;
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
  rc = finishFlushLevel(ch, level, 1, &hash);
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

        if( level>0 && pLevel->builder.nItems==1 ){
          rc = canonicalRootFromSingleChild(ch, &pLevel->builder, &hash);
          if( rc!=SQLITE_OK ) return rc;
          memcpy(&ch->root, &hash, sizeof(ProllyHash));
          resetLevel(pLevel);
          return SQLITE_OK;
        }

        rc = finishFlushLevel(ch, level, 0, &hash);
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

int prollyChunkerAddLeafNode(ProllyChunker *ch,
                             const u8 *pKey, int nKey,
                             u8 *pData, int nData, u64 nItems){
  ProllyHash hash;
  memset(&hash, 0, sizeof(hash));
  if( deferLeaf(ch, pData, nData, 0)!=SQLITE_OK ){
    int rc = chunkStorePut(ch->pStore, pData, nData, &hash);
    sqlite3_free(pData);
    if( rc!=SQLITE_OK ) return rc;
  }
  return addToLevel(ch, 1, pKey, nKey, hash.data, PROLLY_HASH_SIZE, 0, nItems);
}

void prollyChunkerFree(ProllyChunker *ch){
  int i;
  for(i = 0; i < ch->nLeaf; i++) sqlite3_free(ch->aLeaf[i].pData);
  sqlite3_free(ch->aLeaf);
  ch->aLeaf = 0;
  ch->nLeaf = ch->nLeafAlloc = 0;
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
