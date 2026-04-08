/*
** Rolling-hash tree builder for prolly trees.
**
** Builds a prolly tree bottom-up by accepting sorted leaf key-value pairs,
** using rolling hash to determine node boundaries, and recursing upward
** through levels to produce a content-addressed tree.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_chunker.h"
#include "prolly_cursor.h"  /* For PROLLY_CURSOR_MAX_DEPTH */

#include <string.h>
#include <assert.h>

/* Rolling hash window size for boundary detection */
#define CHUNKER_WINDOW_SIZE 64

/*
** Forward declarations of internal helpers.
*/
static int initLevel(ProllyChunker *ch, int level);
static int flushLevel(ProllyChunker *ch, int level);
static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal);
static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash);

/*
** initLevel — Initialize a ProllyChunkerLevel.
**
** Sets up the NodeBuilder with the appropriate tree level and flags,
** and initializes the RollingHash with window size CHUNKER_WINDOW_SIZE.
** Returns SQLITE_OK on success or SQLITE_NOMEM on allocation failure.
*/
static int initLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel;
  int rc;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );

  pLevel = &ch->aLevel[level];
  memset(pLevel, 0, sizeof(ProllyChunkerLevel));

  /* Internal nodes (level>0) have child hashes as values.
  ** Leaf nodes (level==0) have arbitrary payload values.
  ** The builder level corresponds to the tree level. */
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

/*
** Extract the last key from a node builder.  The builder must have
** at least one item.  Sets *ppKey and *pnKey to point into the
** builder's key buffer (valid until the builder is reset or freed).
*/
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

/*
** flushLevel — Finalize the node builder at a given level during
** normal (non-finish) operation.
**
** 1. Extracts the last key (needed for promotion).
** 2. Serializes the node via prollyNodeBuilderFinish.
** 3. Writes the chunk to the ChunkStore via chunkStorePut.
** 4. Promotes the last key + chunk hash to the parent level (level+1)
**    via addToLevel, which may recursively flush higher levels.
** 5. Resets the builder and rolling hash for reuse.
**
** Returns SQLITE_OK on success or an error code.
*/
static int flushLevel(ProllyChunker *ch, int level){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  ProllyHash hash;
  const u8 *pLastKey;
  int nLastKey;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  /* Extract the last key before serialization.  The key data lives in the
  ** builder's key buffer, which remains valid through Finish and until
  ** Reset is called. */
  builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);

  /* Serialize the node */
  rc = prollyNodeBuilderFinish(&pLevel->builder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  /* Write chunk to store, get content hash */
  rc = chunkStorePut(ch->pStore, pData, nData, &hash);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ) return rc;

  /* Promote last key + hash to the parent level.
  ** The parent level (level+1) is an internal node whose values are
  ** child hashes (PROLLY_HASH_SIZE bytes each).
  ** addToLevel may itself trigger further flushes at higher levels. */
  if( level + 1 < PROLLY_CURSOR_MAX_DEPTH ){
    rc = addToLevel(ch, level + 1,
                    pLastKey, nLastKey,
                    hash.data, PROLLY_HASH_SIZE);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* Reset the builder and rolling hash for reuse at this level */
  prollyNodeBuilderReset(&pLevel->builder);
  prollyRollingHashReset(&pLevel->rh);
  pLevel->nItems = 0;
  pLevel->nBytes = 0;

  return SQLITE_OK;
}

/*
** finishFlushLevel — Flush a level during the Finish phase.
**
** Like flushLevel but does NOT promote to parent.  Instead, the
** resulting chunk hash is returned in *pHash and the last key is
** left for the caller to handle promotion.
**
** Returns SQLITE_OK on success or an error code.
*/
static int finishFlushLevel(ProllyChunker *ch, int level,
                            ProllyHash *pHash){
  ProllyChunkerLevel *pLevel = &ch->aLevel[level];
  u8 *pData = 0;
  int nData = 0;
  int rc;

  assert( pLevel->builder.nItems > 0 );

  /* Serialize the node */
  rc = prollyNodeBuilderFinish(&pLevel->builder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  /* Write chunk to store */
  rc = chunkStorePut(ch->pStore, pData, nData, pHash);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

/*
** addToLevel — Add a key-value pair to a specific chunker level.
**
** 1. Ensures the level is initialized (lazy init).
** 2. Adds the key-value pair to the level's node builder.
** 3. Feeds the key bytes into the rolling hash for boundary detection.
** 4. Increments nBytes by (nKey + nVal).
** 5. If nBytes >= PROLLY_CHUNK_MIN AND (rolling hash is at boundary OR
**    nBytes >= PROLLY_CHUNK_MAX), flushes this level.
**
** Returns SQLITE_OK on success or an error code.
*/
static int addToLevel(ProllyChunker *ch, int level,
                      const u8 *pKey, int nKey,
                      const u8 *pVal, int nVal){
  ProllyChunkerLevel *pLevel;
  int rc;
  int i;

  assert( level >= 0 && level < PROLLY_CURSOR_MAX_DEPTH );

  /* Ensure this level is initialized */
  if( level >= ch->nLevels ){
    while( ch->nLevels <= level ){
      rc = initLevel(ch, ch->nLevels);
      if( rc!=SQLITE_OK ) return rc;
      ch->nLevels++;
    }
  }

  pLevel = &ch->aLevel[level];

  /* Add the key-value pair to the node builder */
  rc = prollyNodeBuilderAdd(&pLevel->builder, pKey, nKey, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;

  pLevel->nItems++;

  /* Feed each byte of the key into the rolling hash */
  for(i = 0; i < nKey; i++){
    prollyRollingHashUpdate(&pLevel->rh, pKey[i]);
  }

  /* Accumulate byte count */
  pLevel->nBytes += nKey + nVal;

  /* Check if we should flush this level:
  ** - Must have at least PROLLY_CHUNK_MIN bytes accumulated
  ** - Either the rolling hash has hit a boundary pattern, or
  **   we've exceeded the max chunk size */
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

/*
** prollyChunkerInit — Initialize a chunker for building a new tree.
**
** Zeros the struct and stores the chunk store pointer and flags.
** Levels are not initialized until they are first used (lazy init).
*/
int prollyChunkerInit(ProllyChunker *ch, ChunkStore *pStore, u8 flags){
  memset(ch, 0, sizeof(ProllyChunker));
  ch->pStore = pStore;
  ch->flags = flags;
  ch->nLevels = 0;
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

/*
** prollyChunkerAdd — Add a leaf key-value pair.
**
** Ensures level 0 is initialized and delegates to addToLevel at level 0.
** The chunker handles splitting nodes and building internal levels
** automatically via the rolling hash boundary detection.
*/
int prollyChunkerAdd(ProllyChunker *ch,
                     const u8 *pKey, int nKey,
                     const u8 *pVal, int nVal){
  int rc;

  rc = addToLevel(ch, 0, pKey, nKey, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;

  return SQLITE_OK;
}

/*
** prollyChunkerFinish — Finalize the tree.
**
** Flushes remaining items at each level from bottom up.  The topmost
** level that produces a single node becomes the root.
**
** Algorithm:
**   Walk from level 0 upward.  At each level that has pending items
**   in its builder, serialize and write the node.  If there is a parent
**   level that already has items (from earlier boundary flushes), or if
**   the current level had earlier boundary flushes (meaning the parent
**   already received promotions), promote this final node's last key +
**   hash to the parent.  Otherwise this node is the root.
**
** The key insight: during normal operation, every time a level is
** flushed, its last key + hash is promoted to level+1.  So if level L
** was flushed K times during Add, then level L+1 has at least K entries.
** During Finish, level L may have leftover items.  If L+1 has items
** (either from earlier promotions or from flushing L-1 during this
** Finish pass), we must also promote L's final node to L+1.  If L+1
** has no items and won't get any, then L's node is the root.
**
** Special cases:
**   - Empty tree: root is the zero hash.
**   - All entries fit in one node at some level: that node is the root.
*/
int prollyChunkerFinish(ProllyChunker *ch){
  int rc;
  int level;
  int maxLevel;

  /* Handle empty tree */
  if( ch->nLevels == 0 ){
    memset(&ch->root, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /*
  ** We process levels bottom-up.  Flushing level L during Finish adds
  ** an entry to level L+1, which may increase nLevels.  We must keep
  ** going until we've handled all levels.  We use a while loop since
  ** maxLevel can grow.
  */
  level = 0;
  while( level < ch->nLevels ){
    ProllyChunkerLevel *pLevel = &ch->aLevel[level];

    if( pLevel->builder.nItems == 0 ){
      /* Nothing pending at this level */
      level++;
      continue;
    }

    /* Determine if a parent level exists and has (or will have) items.
    ** The parent has items if:
    **   (a) it was already initialized and has items in its builder, OR
    **   (b) a higher level has items (meaning the tree goes higher).
    **
    ** If neither is true, then this level's single remaining node
    ** is the root candidate.  But we only call it root if no higher
    ** level has pending items.  */
    {
      int parentActive = 0;
      int k;

      /* Check if any level above has pending items */
      for(k = level + 1; k < ch->nLevels; k++){
        if( ch->aLevel[k].builder.nItems > 0 ){
          parentActive = 1;
          break;
        }
      }

      if( parentActive ){
        /* There are items above.  We must serialize this node and
        ** promote its last key + hash to level+1. */
        ProllyHash hash;
        const u8 *pLastKey;
        int nLastKey;

        builderLastKey(&pLevel->builder, &pLastKey, &nLastKey);

        rc = finishFlushLevel(ch, level, &hash);
        if( rc!=SQLITE_OK ) return rc;

        /* Ensure parent level is initialized */
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

        /* Reset this level */
        prollyNodeBuilderReset(&pLevel->builder);
        prollyRollingHashReset(&pLevel->rh);
        pLevel->nItems = 0;
        pLevel->nBytes = 0;
      } else {
        /* No higher level has items.  This node is the root. */
        ProllyHash hash;

        rc = finishFlushLevel(ch, level, &hash);
        if( rc!=SQLITE_OK ) return rc;

        memcpy(&ch->root, &hash, sizeof(ProllyHash));

        /* Clean up this level */
        prollyNodeBuilderReset(&pLevel->builder);
        prollyRollingHashReset(&pLevel->rh);
        pLevel->nItems = 0;
        pLevel->nBytes = 0;
        return SQLITE_OK;
      }
    }

    level++;
  }

  /* Should not reach here with valid input — the loop above should
  ** always find a topmost root.  But handle gracefully. */
  memset(&ch->root, 0, sizeof(ProllyHash));
  return SQLITE_OK;
}

/*
** prollyChunkerGetRoot — Copy the root hash to the output parameter.
*/
void prollyChunkerGetRoot(ProllyChunker *ch, ProllyHash *pRoot){
  memcpy(pRoot, &ch->root, sizeof(ProllyHash));
}

/*
** prollyChunkerAddAtLevel — Public wrapper for addToLevel.
** Allows injecting entries at arbitrary levels of the chunker,
** e.g. inserting (lastKey, childHash) at level 1 to skip
** re-serializing unchanged leaf nodes.
*/
int prollyChunkerAddAtLevel(ProllyChunker *ch, int level,
                            const u8 *pKey, int nKey,
                            const u8 *pVal, int nVal){
  return addToLevel(ch, level, pKey, nKey, pVal, nVal);
}

/*
** prollyChunkerFlushLevel — Flush pending entries at a level.
** If the level has items in its builder, serialize them into a chunk,
** promote to the parent, and reset. This is needed before injecting
** entries at a higher level to maintain consistency.
*/
int prollyChunkerFlushLevel(ProllyChunker *ch, int level){
  if( level >= ch->nLevels ) return SQLITE_OK;
  if( ch->aLevel[level].builder.nItems == 0 ) return SQLITE_OK;
  return flushLevel(ch, level);
}

/*
** prollyChunkerFree — Free all resources held by the chunker.
**
** Frees node builders and rolling hashes at every initialized level.
*/
void prollyChunkerFree(ProllyChunker *ch){
  int i;
  for(i = 0; i < ch->nLevels; i++){
    prollyNodeBuilderFree(&ch->aLevel[i].builder);
    prollyRollingHashFree(&ch->aLevel[i].rh);
  }
  ch->nLevels = 0;
}

#endif /* DOLTLITE_PROLLY */
