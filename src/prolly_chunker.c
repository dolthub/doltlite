
#ifdef DOLTLITE_PROLLY

#include "prolly_chunker.h"
#include "prolly_cursor.h"  

#include <string.h>
#include <assert.h>

#define CHUNKER_WINDOW_SIZE 64

static int initLevel(ProllyChunker *ch, int level);
static int flushLevel(ProllyChunker *ch, int level);
static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal);
static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash);

static int initLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel;
  int rc;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );

  pLevel = &ch->aLevel[level];
  memset(pLevel, 0, sizeof(ProllyChunkerLevel));

  
  prollyNodeBuilderInit(&pLevel->builder, (u8)level, ch->flags);

  rc = prollyRollingHashInit(&pLevel->rh, CHUNKER_WINDOW_SIZE);
  if( rc!=SQLITE_OK ){
    prollyNodeBuilderFree(&pLevel->builder);
    return rc;
  }

  pLevel->nItems = 0;
  pLevel->nBytes = 0;

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

static int flushLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  ProllyHash hash;
  const u8 *pLastKey;
  int nLastKey;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  
  builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);

  
  rc = prollyNodeBuilderFinish(&pLevel->builder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  
  rc = chunkStorePut(ch->pStore, pData, nData, &hash);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ) return rc;

  
  /* Propagate upward: add (lastKey -> childHash) entry to parent level.
  ** The parent's key is the last key from this child, matching prolly tree
  ** convention where internal keys are the max key of their subtree. */
  if( level + 1 < PROLLY_CURSOR_MAX_DEPTH ){
    rc = addToLevel(ch, level + 1,
                    pLastKey, nLastKey,
                    hash.data, PROLLY_HASH_SIZE);
    if( rc!=SQLITE_OK ) return rc;
  }

  
  prollyNodeBuilderReset(&pLevel->builder);
  prollyRollingHashReset(&pLevel->rh);
  pLevel->nItems = 0;
  pLevel->nBytes = 0;

  return SQLITE_OK;
}

static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  
  rc = prollyNodeBuilderFinish(&pLevel->builder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  
  rc = chunkStorePut(ch->pStore, pData, nData, pHash);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal){
  ProllyChunkerLevel *pLevel;
  int rc;
  int i;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );

  
  if( level >= ch->nLevels ){
    while( ch->nLevels <= level ){
      rc = initLevel(ch, ch->nLevels);
      if( rc!=SQLITE_OK ) return rc;
      ch->nLevels++;
    }
  }

  pLevel = &ch->aLevel[level];

  
  rc = prollyNodeBuilderAdd(&pLevel->builder, pKey, nKey, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;

  pLevel->nItems++;

  
  for(i = 0; i < nKey; i++){
    prollyRollingHashUpdate(&pLevel->rh, pKey[i]);
  }

  
  pLevel->nBytes += nKey + nVal;

  
  /* Content-defined chunking: split when the rolling hash of key bytes
  ** matches PROLLY_CHUNK_PATTERN (after reaching PROLLY_CHUNK_MIN bytes).
  ** This makes boundaries depend on content, not position, so insertions
  ** only invalidate nearby chunks rather than shifting all subsequent ones. */
  if( pLevel->nBytes >= PROLLY_CHUNK_MIN ){
    int atBoundary = prollyRollingHashAtBoundary(&pLevel->rh,
                                                  PROLLY_CHUNK_PATTERN);
    if( atBoundary || pLevel->nBytes >= PROLLY_CHUNK_MAX ){
      rc = flushLevel(ch, level);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags){
  memset(ch, 0, sizeof(ProllyChunker));
  ch->pStore = pStore;
  ch->flags = flags;
  ch->nLevels = 0;
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal){
  int rc;

  rc = addToLevel(ch, 0, pKey, nKey, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

/* Flush remaining items bottom-up. At each level, if a parent level has
** pending items, finalize this level's node and add its hash to the parent.
** The highest level with items becomes the tree root. */
int prollyChunkerFinish(ProllyChunker *ch){
  int rc;
  int level;
  int maxLevel;

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
      int parentActive = 0;
      int k;

      
      for(k = level + 1; k < ch->nLevels; k++){
        if( ch->aLevel[k].builder.nItems > 0 ){
          parentActive = 1;
          break;
        }
      }

      if( parentActive ){
        
        ProllyHash hash;
        const u8 *pLastKey;
        int nLastKey;

        builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);

        rc = finishFlushLevel(ch, level, &hash);
        if( rc!=SQLITE_OK ) return rc;

        
        if( level + 1 >= ch->nLevels ){
          rc = initLevel(ch, ch->nLevels);
          if( rc!=SQLITE_OK ) return rc;
          ch->nLevels++;
        }

        rc = prollyNodeBuilderAdd(
          &ch->aLevel[level + 1].builder,
          pLastKey, nLastKey,
          hash.data, PROLLY_HASH_SIZE
        );
        if( rc!=SQLITE_OK ) return rc;
        ch->aLevel[level + 1].nItems++;

        
        prollyNodeBuilderReset(&pLevel->builder);
        prollyRollingHashReset(&pLevel->rh);
        pLevel->nItems = 0;
        pLevel->nBytes = 0;
      } else {
        
        ProllyHash hash;

        rc = finishFlushLevel(ch, level, &hash);
        if( rc!=SQLITE_OK ) return rc;

        memcpy(&ch->root, &hash, sizeof(ProllyHash));

        
        prollyNodeBuilderReset(&pLevel->builder);
        prollyRollingHashReset(&pLevel->rh);
        pLevel->nItems = 0;
        pLevel->nBytes = 0;
        return SQLITE_OK;
      }
    }

    level++;
  }

  
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot){
  memcpy(pRoot, &ch->root, sizeof(ProllyHash));
}

int prollyChunkerAddAtLevel(ProllyChunker *ch, int level,
                            const u8 *pKey, int nKey,
                            const u8 *pVal, int nVal){
  return addToLevel(ch, level, pKey, nKey, pVal, nVal);
}

int prollyChunkerFlushLevel(ProllyChunker *ch, int level){
  if( level >= ch->nLevels ) return SQLITE_OK;
  if( ch->aLevel[level].builder.nItems == 0 ) return SQLITE_OK;
  return flushLevel(ch, level);
}

void prollyChunkerFree(ProllyChunker *ch){
  int i;
  for(i = 0; i < ch->nLevels; i++){
    prollyNodeBuilderFree(&ch->aLevel[i].builder);
    prollyRollingHashFree(&ch->aLevel[i].rh);
  }
  ch->nLevels = 0;
}

#endif 
