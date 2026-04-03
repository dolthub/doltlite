/*
** Merge-flush: apply buffered edits from MutMap into an existing prolly tree.
** This is the core mutation algorithm — it merges sorted edits with the
** existing tree's leaf entries, then rebuilds the tree using the chunker.
**
** The algorithm performs a single sorted merge-walk over two streams:
**   1. The existing tree's leaf entries (via cursor)
**   2. The pending edits (via MutMap iterator)
**
** The merged output is fed to the chunker, which produces a new set of
** content-addressed nodes forming the updated tree. Unchanged subtrees
** are structurally shared with the old tree.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

/*
** Encode a 64-bit integer as 8 bytes in little-endian format.
** Used to convert INTKEY values into a sortable byte representation
** for the chunker and node builder.
*/
/*
** Encode a signed 64-bit integer as 8 bytes in BIG-ENDIAN order.
** Big-endian is required so that memcmp on encoded keys gives the
** same order as numeric comparison — the prolly tree relies on
** lexicographic key ordering.
**
** For signed integers, we XOR the sign bit so that negative values
** sort before positive values in unsigned byte comparison:
**   -1 → 7F FF FF FF FF FF FF FF
**    0 → 80 00 00 00 00 00 00 00
**    1 → 80 00 00 00 00 00 00 01
*/
static void encodeI64BE(u8 *buf, i64 v){
  u64 u = (u64)v ^ ((u64)1 << 63);  /* flip sign bit */
  buf[0] = (u8)(u >> 56);
  buf[1] = (u8)(u >> 48);
  buf[2] = (u8)(u >> 40);
  buf[3] = (u8)(u >> 32);
  buf[4] = (u8)(u >> 24);
  buf[5] = (u8)(u >> 16);
  buf[6] = (u8)(u >> 8);
  buf[7] = (u8)(u);
}

/*
** Estimated entries per leaf node, used for tree size estimation
** in the merge-vs-apply heuristic.
*/
#define PROLLY_EST_ENTRIES_PER_LEAF 50

/*
** Compare two keys according to the node flags.
**
** For INTKEY tables: compare the 64-bit integer keys.
** For BLOBKEY tables: keys are sort keys — memcmp gives correct order.
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
static int compareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( iKey1 < iKey2 ) return -1;
    if( iKey1 > iKey2 ) return +1;
    return 0;
  }else{
    int n = nKey1 < nKey2 ? nKey1 : nKey2;
    int c = memcmp(pKey1, pKey2, n);
    if( c != 0 ) return c;
    if( nKey1 < nKey2 ) return -1;
    if( nKey1 > nKey2 ) return 1;
    return 0;
  }
}

/*
** Feed a single key-value pair into the chunker.
**
** For INTKEY tables the key is serialized as an 8-byte little-endian i64
** so that the chunker (which works on raw bytes) receives a sortable key
** representation.  For BLOBKEY tables the key bytes are passed through
** directly.
*/
static int feedChunker(
  ProllyChunker *pCh,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  if( flags & PROLLY_NODE_INTKEY ){
    /* Encode integer key as 8-byte little-endian for the chunker. */
    u8 aKeyBuf[8];
    encodeI64BE(aKeyBuf, intKey);
    return prollyChunkerAdd(pCh, aKeyBuf, 8, pVal, nVal);
  }else{
    return prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
  }
}

/*
** Build a new tree from scratch using only the edits in the MutMap.
** This is used when the old tree is empty (oldRoot is all-zeros).
**
** We simply iterate over all edits in sorted order. INSERT edits are
** fed to the chunker; DELETE edits are silently ignored (there is
** nothing to delete in an empty tree).
*/
static int buildFromEdits(
  ProllyMutator *pMut
){
  ProllyChunker chunker;
  ProllyMutMapIter iter;
  int rc;

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ) return rc;

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags,
                       pEntry->pKey, pEntry->nKey, pEntry->intKey,
                       pEntry->pVal, pEntry->nVal);
      if( rc!=SQLITE_OK ){
        prollyChunkerFree(&chunker);
        return rc;
      }
    }
    prollyMutMapIterNext(&iter);
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

  prollyChunkerFree(&chunker);
  return rc;
}

static int mergeWalk(ProllyMutator *pMut);

/*
** Check if the next edit key is within a subtree's key range.
** Returns 1 if the edit key <= the subtree's boundary key (has edits),
** 0 if the edit key > boundary key (no edits in this subtree).
** If no edits remain, returns 0.
*/
static int subtreeHasEdits(
  u8 flags,
  ProllyMutMapIter *pIter,
  const u8 *pBoundKey, int nBoundKey, i64 iBoundKey
){
  ProllyMutMapEntry *pEd;
  int cmp;
  if( !prollyMutMapIterValid(pIter) ) return 0;
  pEd = prollyMutMapIterEntry(pIter);
  cmp = compareKeys(flags, pEd->pKey, pEd->nKey, pEd->intKey,
                    pBoundKey, nBoundKey, iBoundKey);
  return (cmp <= 0);  /* edit key <= boundary key means edits in this subtree */
}

/*
** Merge a single leaf node's entries with the edit iterator.
** Feeds merged entries into the chunker at level 0.
** Advances the edit iterator past all edits consumed.
*/
static int mergeLeaf(
  ProllyMutator *pMut,
  ProllyNode *pLeaf,
  ProllyChunker *pCh,
  ProllyMutMapIter *pIter
){
  int rc = SQLITE_OK;
  int j;
  u8 flags = pMut->flags;

  for( j = 0; j < pLeaf->nItems; ){
    int haveEdit = prollyMutMapIterValid(pIter);
    ProllyMutMapEntry *pEd = haveEdit ? prollyMutMapIterEntry(pIter) : 0;

    const u8 *pCurKey; int nCurKey;
    i64 iCurKey = 0;
    u8 aKeyBuf[8];

    if( flags & PROLLY_NODE_INTKEY ){
      iCurKey = prollyNodeIntKey(pLeaf, j);
      encodeI64BE(aKeyBuf, iCurKey);
      pCurKey = aKeyBuf; nCurKey = 8;
    }else{
      prollyNodeKey(pLeaf, j, &pCurKey, &nCurKey);
    }

    if( !haveEdit ){
      /* No more edits — emit remaining leaf entries */
      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
      continue;
    }

    /* Check if edit is past this leaf */
    {
      const u8 *pLastKey; int nLastKey;
      i64 iLastKey = 0;
      u8 aLastBuf[8];
      if( flags & PROLLY_NODE_INTKEY ){
        iLastKey = prollyNodeIntKey(pLeaf, pLeaf->nItems - 1);
        encodeI64BE(aLastBuf, iLastKey);
        pLastKey = aLastBuf; nLastKey = 8;
      }else{
        prollyNodeKey(pLeaf, pLeaf->nItems - 1, &pLastKey, &nLastKey);
      }
      int pastLeaf = compareKeys(flags, pEd->pKey, pEd->nKey, pEd->intKey,
                                 pLastKey, nLastKey, iLastKey);
      if( pastLeaf > 0 ){
        /* Edit is past this leaf — emit remaining entries and return */
        const u8 *pVal; int nVal;
        prollyNodeValue(pLeaf, j, &pVal, &nVal);
        rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }
    }

    /* Compare current entry with edit */
    int cmp = compareKeys(flags, pCurKey, nCurKey, iCurKey,
                          pEd->pKey, pEd->nKey, pEd->intKey);
    if( cmp < 0 ){
      /* Old entry first */
      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else if( cmp == 0 ){
      /* Replace or delete */
      if( pEd->op==PROLLY_EDIT_INSERT ){
        u8 aEditKey[8];
        const u8 *pEK; int nEK;
        if( flags & PROLLY_NODE_INTKEY ){
          encodeI64BE(aEditKey, pEd->intKey);
          pEK = aEditKey; nEK = 8;
        }else{
          pEK = pEd->pKey; nEK = pEd->nKey;
        }
        rc = prollyChunkerAdd(pCh, pEK, nEK, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      j++;
      prollyMutMapIterNext(pIter);
    }else{
      /* Insert new entry before old */
      if( pEd->op==PROLLY_EDIT_INSERT ){
        u8 aEditKey[8];
        const u8 *pEK; int nEK;
        if( flags & PROLLY_NODE_INTKEY ){
          encodeI64BE(aEditKey, pEd->intKey);
          pEK = aEditKey; nEK = 8;
        }else{
          pEK = pEd->pKey; nEK = pEd->nKey;
        }
        rc = prollyChunkerAdd(pCh, pEK, nEK, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      prollyMutMapIterNext(pIter);
    }
  }
  return SQLITE_OK;
}

/*
** Streaming merge: walk the old tree's internal nodes alongside the edit
** iterator. Skip unchanged subtrees by emitting their hash directly at
** the parent level. Only descend into subtrees that contain edits.
**
** For height-1 trees (root → leaves): O(M * L + S) where M = edits,
** L = leaf size, S = number of skipped subtrees. Much faster than
** mergeWalk's O(N + M) when M << N.
**
** For height-0 trees (leaf root): falls through to mergeWalk.
*/
static int streamingMerge(
  ProllyMutator *pMut
){
  ProllyChunker chunker;
  ProllyMutMapIter iter;
  int rc;
  u8 *pRootData = 0;
  int nRootData = 0;
  ProllyNode rootNode;
  ProllyCache *pCache = pMut->pCache;

  /* Load root node */
  rc = chunkStoreGet(pMut->pStore, &pMut->oldRoot, &pRootData, &nRootData);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(&rootNode, pRootData, nRootData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pRootData);
    return rc;
  }

  /* Height-0 tree: single leaf, use mergeWalk */
  if( rootNode.level == 0 ){
    sqlite3_free(pRootData);
    return mergeWalk(pMut);
  }

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pRootData);
    return rc;
  }

  /* Walk root node's children (level-1 entries) */
  {
    int i;
    for( i = 0; i < rootNode.nItems; i++ ){
      const u8 *pBoundKey; int nBoundKey;
      const u8 *pChildVal; int nChildVal;
      i64 iBoundKey = 0;
      u8 aBoundBuf[8];

      prollyNodeKey(&rootNode, i, &pBoundKey, &nBoundKey);
      prollyNodeValue(&rootNode, i, &pChildVal, &nChildVal);

      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iBoundKey = prollyNodeIntKey(&rootNode, i);
        encodeI64BE(aBoundBuf, iBoundKey);
        pBoundKey = aBoundBuf; nBoundKey = 8;
      }

      if( !subtreeHasEdits(pMut->flags, &iter,
                           pBoundKey, nBoundKey, iBoundKey)
       && (chunker.nLevels == 0
           || chunker.aLevel[0].builder.nItems == 0) ){
        /* No edits in this subtree AND level-0 is empty (naturally
        ** at a chunk boundary). Skip by emitting hash at parent level.
        ** If level-0 has pending items, we must descend to preserve
        ** chunk boundary alignment with the original tree. */
        {
          int lv;
          for( lv = 0; lv < rootNode.level; lv++ ){
            rc = prollyChunkerFlushLevel(&chunker, lv);
            if( rc!=SQLITE_OK ) goto streaming_cleanup;
          }
        }
        rc = prollyChunkerAddAtLevel(&chunker, rootNode.level,
                                      pBoundKey, nBoundKey,
                                      pChildVal, nChildVal);
        if( rc!=SQLITE_OK ) goto streaming_cleanup;
      }else{
        /* Subtree has edits — load child and merge at leaf level */
        if( rootNode.level == 1 ){
          /* Child is a leaf — merge directly */
          ProllyHash childHash;
          ProllyCacheEntry *pChildEntry;
          u8 *pChildData = 0;
          int nChildData = 0;

          assert( nChildVal == PROLLY_HASH_SIZE );
          memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
          pChildEntry = prollyCacheGet(pCache, &childHash);
          if( !pChildEntry ){
            rc = chunkStoreGet(pMut->pStore, &childHash, &pChildData, &nChildData);
            if( rc!=SQLITE_OK ) goto streaming_cleanup;
            pChildEntry = prollyCachePut(pCache, &childHash, pChildData, nChildData);
            sqlite3_free(pChildData);
            if( !pChildEntry ){ rc = SQLITE_NOMEM; goto streaming_cleanup; }
          }

          rc = mergeLeaf(pMut, &pChildEntry->node, &chunker, &iter);
          prollyCacheRelease(pCache, pChildEntry);
          if( rc!=SQLITE_OK ) goto streaming_cleanup;
        }else{
          /* TODO: recurse for height > 2 trees. For now, fall back
          ** to walking all entries in this subtree. */
          ProllyCursor subCur;
          ProllyHash childHash;
          int subEmpty;

          assert( nChildVal == PROLLY_HASH_SIZE );
          memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
          prollyCursorInit(&subCur, pMut->pStore, pMut->pCache,
                           &childHash, pMut->flags);
          rc = prollyCursorFirst(&subCur, &subEmpty);
          if( rc!=SQLITE_OK ){
            prollyCursorClose(&subCur);
            goto streaming_cleanup;
          }
          while( prollyCursorIsValid(&subCur) ){
            const u8 *pK; int nK;
            const u8 *pV; int nV;
            i64 iK = 0;
            if( pMut->flags & PROLLY_NODE_INTKEY ){
              iK = prollyCursorIntKey(&subCur);
              pK = 0; nK = 0;
            }else{
              prollyCursorKey(&subCur, &pK, &nK);
            }
            prollyCursorValue(&subCur, &pV, &nV);

            /* Check if current entry should be merged with edit */
            if( prollyMutMapIterValid(&iter) ){
              ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);
              int cmp = compareKeys(pMut->flags, pK, nK, iK,
                                    pEd->pKey, pEd->nKey, pEd->intKey);
              if( cmp < 0 ){
                rc = feedChunker(&chunker, pMut->flags, pK, nK, iK, pV, nV);
                if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
                rc = prollyCursorNext(&subCur);
                if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
              }else if( cmp == 0 ){
                if( pEd->op==PROLLY_EDIT_INSERT ){
                  rc = feedChunker(&chunker, pMut->flags,
                                   pEd->pKey, pEd->nKey, pEd->intKey,
                                   pEd->pVal, pEd->nVal);
                  if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
                }
                rc = prollyCursorNext(&subCur);
                if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
                prollyMutMapIterNext(&iter);
              }else{
                if( pEd->op==PROLLY_EDIT_INSERT ){
                  rc = feedChunker(&chunker, pMut->flags,
                                   pEd->pKey, pEd->nKey, pEd->intKey,
                                   pEd->pVal, pEd->nVal);
                  if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
                }
                prollyMutMapIterNext(&iter);
              }
            }else{
              rc = feedChunker(&chunker, pMut->flags, pK, nK, iK, pV, nV);
              if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
              rc = prollyCursorNext(&subCur);
              if( rc!=SQLITE_OK ){ prollyCursorClose(&subCur); goto streaming_cleanup; }
            }
          }
          prollyCursorClose(&subCur);
        }
      }
    }
  }

  /* Handle remaining edits past the tree's last key */
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);
    if( pEd->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags,
                       pEd->pKey, pEd->nKey, pEd->intKey,
                       pEd->pVal, pEd->nVal);
      if( rc!=SQLITE_OK ) goto streaming_cleanup;
    }
    prollyMutMapIterNext(&iter);
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

streaming_cleanup:
  prollyChunkerFree(&chunker);
  sqlite3_free(pRootData);
  return rc;
}

/*
** Merge-walk the existing tree with the edit map and produce a new tree.
** O(N + M) — walks every entry regardless of edit count.
** Used when M is large relative to N (hybrid flush fallback).
*/
static int mergeWalk(
  ProllyMutator *pMut
){
  ProllyCursor cur;
  ProllyMutMapIter iter;
  ProllyChunker chunker;
  int rc;
  int curEmpty = 0;
  int curValid;
  int iterValid;

  /* prollyCursorInit returns void — it just sets fields, cannot fail. */
  prollyCursorInit(&cur, pMut->pStore, pMut->pCache,
                   &pMut->oldRoot, pMut->flags);
  rc = prollyCursorFirst(&cur, &curEmpty);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( curEmpty ){
    prollyCursorClose(&cur);
    return buildFromEdits(pMut);
  }

  prollyMutMapIterFirst(&iter, pMut->pEdits);

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }

  for(;;){
    curValid = prollyCursorIsValid(&cur);
    iterValid = prollyMutMapIterValid(&iter);
    if( !curValid && !iterValid ) break;

    if( curValid && !iterValid ){
      const u8 *pKey; int nKey;
      const u8 *pVal; int nVal;
      i64 intKey = 0;
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        intKey = prollyCursorIntKey(&cur);
        pKey = 0; nKey = 0;
      }else{
        prollyCursorKey(&cur, &pKey, &nKey);
      }
      prollyCursorValue(&cur, &pVal, &nVal);
      rc = feedChunker(&chunker, pMut->flags, pKey, nKey, intKey, pVal, nVal);
      if( rc!=SQLITE_OK ) goto merge_cleanup;
      rc = prollyCursorNext(&cur);
      if( rc!=SQLITE_OK ) goto merge_cleanup;
      continue;
    }

    if( !curValid && iterValid ){
      ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
      if( pEntry->op==PROLLY_EDIT_INSERT ){
        rc = feedChunker(&chunker, pMut->flags,
                         pEntry->pKey, pEntry->nKey, pEntry->intKey,
                         pEntry->pVal, pEntry->nVal);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
      }
      prollyMutMapIterNext(&iter);
      continue;
    }

    {
      ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
      const u8 *pCurKey; int nCurKey;
      i64 iCurKey = 0;
      int cmp;

      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iCurKey = prollyCursorIntKey(&cur);
        pCurKey = 0; nCurKey = 0;
      }else{
        prollyCursorKey(&cur, &pCurKey, &nCurKey);
      }

      cmp = compareKeys(pMut->flags,
                         pCurKey, nCurKey, iCurKey,
                         pEntry->pKey, pEntry->nKey, pEntry->intKey);

      if( cmp < 0 ){
        const u8 *pVal; int nVal;
        prollyCursorValue(&cur, &pVal, &nVal);
        rc = feedChunker(&chunker, pMut->flags,
                         pCurKey, nCurKey, iCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
        rc = prollyCursorNext(&cur);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
      }else if( cmp == 0 ){
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          rc = feedChunker(&chunker, pMut->flags,
                           pEntry->pKey, pEntry->nKey, pEntry->intKey,
                           pEntry->pVal, pEntry->nVal);
          if( rc!=SQLITE_OK ) goto merge_cleanup;
        }
        rc = prollyCursorNext(&cur);
        if( rc!=SQLITE_OK ) goto merge_cleanup;
        prollyMutMapIterNext(&iter);
      }else{
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          rc = feedChunker(&chunker, pMut->flags,
                           pEntry->pKey, pEntry->nKey, pEntry->intKey,
                           pEntry->pVal, pEntry->nVal);
          if( rc!=SQLITE_OK ) goto merge_cleanup;
        }
        prollyMutMapIterNext(&iter);
      }
    }
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

merge_cleanup:
  prollyChunkerFree(&chunker);
  prollyCursorClose(&cur);
  return rc;
}

/*
** Apply all edits in pMut->pEdits to the tree rooted at pMut->oldRoot.
** Produces a new tree whose root hash is stored in pMut->newRoot.
**
** Three cases:
**   1. No edits: newRoot = oldRoot (no work).
**   2. Old tree is empty: build from edits alone.
**   3. Otherwise: merge-walk old tree + edits.
**
** Returns SQLITE_OK on success with result in pMut->newRoot.
*/
int prollyMutateFlush(ProllyMutator *pMut){
  /* Case 1: no edits — the tree is unchanged */
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /* Case 2: old tree is empty — build entirely from edits */
  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  /* Case 3: pick strategy based on edit count vs tree size.
  **
  ** streamingMerge: O(M * L) — skips unchanged subtrees, great when M << N
  ** mergeWalk:      O(N + M) — walks all entries, better when M ≈ N
  **
  ** Estimate N from the root node to choose the threshold. */
  {
    int M = prollyMutMapCount(pMut->pEdits);
    int N = 0;

    /* Load root to estimate N */
    u8 *pRootData = 0;
    int nRootData = 0;
    int rcEst = chunkStoreGet(pMut->pStore, &pMut->oldRoot,
                              &pRootData, &nRootData);
    if( rcEst==SQLITE_OK && pRootData ){
      ProllyNode rootNode;
      if( prollyNodeParse(&rootNode, pRootData, nRootData)==SQLITE_OK ){
        if( rootNode.level==0 ){
          N = rootNode.nItems;
        }else if( rootNode.level==1 ){
          N = rootNode.nItems * PROLLY_EST_ENTRIES_PER_LEAF;
        }else{
          /* Height >= 2: large tree */
          int factor = 1;
          int lv;
          for(lv = 0; lv < rootNode.level; lv++) factor *= PROLLY_EST_ENTRIES_PER_LEAF;
          N = rootNode.nItems * factor;
        }
      }
      sqlite3_free(pRootData);
    }

    /* Threshold: use mergeWalk when edits exceed ~50% of estimated tree size.
    ** With sort key encoding, BLOBKEY comparison is memcmp (same cost as
    ** INTKEY), so both table types use the same threshold. */
    int threshold;
    if( N <= 0 ){
      threshold = 1000;  /* Fallback if estimation fails */
    }else{
      threshold = N / 2;
    }

    /* Use streamingMerge for small edit sets — it skips unchanged
    ** subtrees for O(M * L) instead of mergeWalk's O(N + M).
    ** Fall back to mergeWalk when edits are a large fraction of the tree. */
    if( M > threshold ){
      return mergeWalk(pMut);
    }else{
      return streamingMerge(pMut);
    }
  }
}

/*
** Convenience function: apply a single INSERT to a tree.
**
** Creates a temporary 1-entry MutMap, wraps it in a ProllyMutator,
** calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateInsert(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the INSERT */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

/*
** Convenience function: apply a single DELETE to a tree.
**
** Creates a temporary 1-entry MutMap with a DELETE edit, wraps it in a
** ProllyMutator, calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateDelete(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the DELETE */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
