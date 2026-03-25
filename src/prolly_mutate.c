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
static void encodeI64LE(u8 *buf, i64 v){
  buf[0] = (u8)(v);
  buf[1] = (u8)(v >> 8);
  buf[2] = (u8)(v >> 16);
  buf[3] = (u8)(v >> 24);
  buf[4] = (u8)(v >> 32);
  buf[5] = (u8)(v >> 40);
  buf[6] = (u8)(v >> 48);
  buf[7] = (u8)(v >> 56);
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
    encodeI64LE(aKeyBuf, intKey);
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
** Clone all entries from a ProllyNode into a ProllyNodeBuilder.
*/
static int cloneNodeToBuilder(
  const ProllyNode *pNode,
  ProllyNodeBuilder *b
){
  int i, rc;
  for(i = 0; i < pNode->nItems; i++){
    const u8 *pK, *pV;
    int nK, nV;
    prollyNodeKey(pNode, i, &pK, &nK);
    prollyNodeValue(pNode, i, &pV, &nV);
    rc = prollyNodeBuilderAdd(b, pK, nK, pV, nV);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/*
** Serialize a node builder, write chunk to store, return hash.
*/
static int builderToChunk(
  ProllyNodeBuilder *b,
  ChunkStore *pStore,
  ProllyHash *pHash
){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  rc = prollyNodeBuilderFinish(b, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStorePut(pStore, pData, nData, pHash);
  sqlite3_free(pData);
  return rc;
}

/*
** Get the key for an entry in a node, handling INTKEY encoding.
** For INTKEY, writes 8-byte LE into aKeyBuf.
*/
static void getNodeKey(
  const ProllyNode *pNode, int idx, u8 flags,
  u8 *aKeyBuf, const u8 **ppKey, int *pnKey
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 ik = prollyNodeIntKey(pNode, idx);
    encodeI64LE(aKeyBuf, ik);
    *ppKey = aKeyBuf;
    *pnKey = 8;
  }else{
    prollyNodeKey(pNode, idx, ppKey, pnKey);
  }
}

/*
** Maximum number of leaf splits when a rebuilt leaf overflows.
*/
#define MAX_LEAF_SPLITS 64

/*
** Result of handleLeafSplits: contains the hashes and boundary keys
** produced when a rebuilt leaf is serialized (possibly splitting).
*/
typedef struct LeafSplitResult LeafSplitResult;
struct LeafSplitResult {
  ProllyHash aLeafHash[MAX_LEAF_SPLITS];
  u8 (*aLeafKeyBuf)[256];       /* heap-allocated, caller must free */
  int aLeafKeyLen[MAX_LEAF_SPLITS];
  int nLeafSplits;
  ProllyHash newHash;            /* single-node hash (or first split) */
};

/*
** Phase 2: Sorted merge of old leaf entries with pending edits.
**
** Walks the old leaf (indices 0..nItems-1) and the edit iterator
** simultaneously, emitting merged entries into leafBuilder.  Edits
** that fall beyond this leaf's key range are left unconsumed for the
** next seek, unless this is the rightmost leaf.
**
** On return the iterator has been advanced past all edits consumed.
*/
static int applyEditsToLeaf(
  ProllyMutator *pMut,
  ProllyNode *pLeaf,
  ProllyCursor *pCur,
  int leafLevel,
  ProllyMutMapIter *pIter,
  ProllyNodeBuilder *pBuilder
){
  int rc = SQLITE_OK;
  int j = 0;

  /* Save iterator position to detect if merge consumed any edits */
  ProllyMutMapEntry *pIterBefore = prollyMutMapIterValid(pIter) ?
                                   prollyMutMapIterEntry(pIter) : 0;

  while( j < pLeaf->nItems || prollyMutMapIterValid(pIter) ){
    int haveOld = (j < pLeaf->nItems);
    int haveEdit = prollyMutMapIterValid(pIter);
    ProllyMutMapEntry *pE = haveEdit ?
                            prollyMutMapIterEntry(pIter) : 0;

    if( haveOld && haveEdit ){
      /* Check if edit is still within this leaf's key range */
      const u8 *pLK; int nLK; i64 iLK = 0;
      u8 aLeafKey[8];
      getNodeKey(pLeaf, pLeaf->nItems - 1, pMut->flags,
                 aLeafKey, &pLK, &nLK);

      int editCmp;
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        i64 lastIK = prollyNodeIntKey(pLeaf, pLeaf->nItems - 1);
        if( pE->intKey > lastIK ) editCmp = 1;
        else editCmp = 0;
      }else{
        editCmp = compareKeys(pMut->flags, pLK, nLK, 0,
                              pE->pKey, pE->nKey, pE->intKey);
        editCmp = (editCmp < 0) ? 1 : 0;  /* 1 = past leaf */
      }

      if( editCmp ){
        /* Edit is past this leaf — copy remaining old entries */
        const u8 *pK; int nK;
        const u8 *pV; int nV;
        u8 aKeyBuf[8];
        getNodeKey(pLeaf, j, pMut->flags, aKeyBuf, &pK, &nK);
        prollyNodeValue(pLeaf, j, &pV, &nV);
        rc = prollyNodeBuilderAdd(pBuilder, pK, nK, pV, nV);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }

      /* Compare old key with edit key */
      const u8 *pOldKey; int nOldKey; i64 iOldKey = 0;
      u8 aOldKey[8];
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iOldKey = prollyNodeIntKey(pLeaf, j);
      }
      getNodeKey(pLeaf, j, pMut->flags, aOldKey, &pOldKey, &nOldKey);

      int cmp = compareKeys(pMut->flags, pOldKey, nOldKey, iOldKey,
                            pE->pKey, pE->nKey, pE->intKey);
      if( cmp < 0 ){
        /* Old entry first — copy */
        const u8 *pV; int nV;
        prollyNodeValue(pLeaf, j, &pV, &nV);
        rc = prollyNodeBuilderAdd(pBuilder, pOldKey, nOldKey, pV, nV);
        if( rc!=SQLITE_OK ) return rc;
        j++;
      }else if( cmp == 0 ){
        /* Replace or delete */
        if( pE->op==PROLLY_EDIT_INSERT ){
          /* INTKEY: reuse existing encoded key (identical for matching keys).
          ** BLOBKEY: use the edit's key bytes and value. */
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            rc = prollyNodeBuilderAdd(pBuilder, pOldKey, nOldKey,
                                      pE->pVal, pE->nVal);
          }else{
            rc = prollyNodeBuilderAdd(pBuilder,
                   pE->pKey, pE->nKey, pE->pVal, pE->nVal);
          }
          if( rc!=SQLITE_OK ) return rc;
        }
        /* DELETE: skip both */
        j++;
        prollyMutMapIterNext(pIter);
      }else{
        /* Insert new entry before old */
        if( pE->op==PROLLY_EDIT_INSERT ){
          u8 aEditKey[8];
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            encodeI64LE(aEditKey, pE->intKey);
            rc = prollyNodeBuilderAdd(pBuilder, aEditKey, 8,
                                      pE->pVal, pE->nVal);
          }else{
            rc = prollyNodeBuilderAdd(pBuilder,
                   pE->pKey, pE->nKey, pE->pVal, pE->nVal);
          }
          if( rc!=SQLITE_OK ) return rc;
        }
        prollyMutMapIterNext(pIter);
      }
    }else if( haveOld ){
      const u8 *pK; int nK;
      const u8 *pV; int nV;
      u8 aKeyBuf[8];
      getNodeKey(pLeaf, j, pMut->flags, aKeyBuf, &pK, &nK);
      prollyNodeValue(pLeaf, j, &pV, &nV);
      rc = prollyNodeBuilderAdd(pBuilder, pK, nK, pV, nV);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else{
      /* Remaining edits are past this leaf's last key.
      ** If this is the rightmost leaf (cursor would go to EOF
      ** after it), consume them as trailing inserts. Otherwise,
      ** break — the next seek will route them to the right leaf. */
      int isRightmost = 1;
      {
        int lv;
        for(lv = leafLevel - 1; lv >= 0; lv--){
          if( pCur->aLevel[lv].pEntry &&
              pCur->aLevel[lv].idx < pCur->aLevel[lv].pEntry->node.nItems - 1 ){
            isRightmost = 0;
            break;
          }
        }
      }
      if( !isRightmost ) break;

      /* This is the rightmost leaf — consume remaining edits */
      while( prollyMutMapIterValid(pIter) ){
        ProllyMutMapEntry *pT = prollyMutMapIterEntry(pIter);
        if( pT->op==PROLLY_EDIT_INSERT ){
          u8 aEditKey[8];
          const u8 *pEK; int nEK;
          if( pMut->flags & PROLLY_NODE_INTKEY ){
            encodeI64LE(aEditKey, pT->intKey);
            pEK = aEditKey; nEK = 8;
          }else{
            pEK = pT->pKey; nEK = pT->nKey;
          }
          rc = prollyNodeBuilderAdd(pBuilder,
                 pEK, nEK, pT->pVal, pT->nVal);
          if( rc!=SQLITE_OK ) return rc;
        }
        prollyMutMapIterNext(pIter);
      }
      break;
    }
  }

  /* Safety: if the merge loop didn't consume ANY edits, force-consume
  ** the current one to prevent infinite loops. */
  {
    ProllyMutMapEntry *pIterAfter = prollyMutMapIterValid(pIter) ?
                                    prollyMutMapIterEntry(pIter) : 0;
    if( pIterAfter == pIterBefore && pIterAfter != 0 ){
      if( pIterAfter->op==PROLLY_EDIT_INSERT ){
        u8 aEditKey[8];
        const u8 *pEK; int nEK;
        if( pMut->flags & PROLLY_NODE_INTKEY ){
          encodeI64LE(aEditKey, pIterAfter->intKey);
          pEK = aEditKey; nEK = 8;
        }else{
          pEK = pIterAfter->pKey; nEK = pIterAfter->nKey;
        }
        rc = prollyNodeBuilderAdd(pBuilder,
               pEK, nEK, pIterAfter->pVal, pIterAfter->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      prollyMutMapIterNext(pIter);
    }
  }

  return SQLITE_OK;
}

/*
** Phase 3: Serialize the rebuilt leaf, splitting if it exceeds the
** chunk size limit.  Uses a rolling hash to find split boundaries.
**
** On success, pResult is populated with the split hashes and boundary
** keys.  The caller must free pResult->aLeafKeyBuf via sqlite3_free.
*/
static int handleLeafSplits(
  ProllyMutator *pMut,
  ProllyNodeBuilder *pBuilder,
  LeafSplitResult *pResult
){
  int rc;

  memset(pResult, 0, sizeof(*pResult));
  pResult->aLeafKeyBuf = (u8(*)[256])sqlite3_malloc(MAX_LEAF_SPLITS * 256);
  if( !pResult->aLeafKeyBuf ) return SQLITE_NOMEM;

  if( pBuilder->nItems == 0 ){
    memset(&pResult->newHash, 0, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  int nOff = (pBuilder->nItems + 1) * 4;
  int nTotal = 8 + nOff * 2 + pBuilder->nKeyBytes + pBuilder->nValBytes;
  if( nTotal <= PROLLY_CHUNK_MAX ){
    rc = builderToChunk(pBuilder, pMut->pStore, &pResult->newHash);
    if( rc!=SQLITE_OK ) return rc;
    pResult->nLeafSplits = 1;
    return SQLITE_OK;
  }

  /* Leaf overflow: split into multiple level-0 nodes using
  ** rolling hash boundaries. */
  ProllyRollingHash rh;
  rc = prollyRollingHashInit(&rh, 64);
  if( rc!=SQLITE_OK ) return rc;

  ProllyNodeBuilder splitB;
  prollyNodeBuilderInit(&splitB, 0, pMut->flags);
  int splitBytes = 0;

  int ei;
  for(ei = 0; ei < pBuilder->nItems; ei++){
    u32 kO0 = pBuilder->aKeyOff[ei];
    u32 kO1 = pBuilder->aKeyOff[ei + 1];
    u32 vO0 = pBuilder->aValOff[ei];
    u32 vO1 = pBuilder->aValOff[ei + 1];
    int kLen = (int)(kO1 - kO0);
    int vLen = (int)(vO1 - vO0);
    const u8 *pKB = pBuilder->pKeyBuf + kO0;
    const u8 *pVB = pBuilder->pValBuf + vO0;

    rc = prollyNodeBuilderAdd(&splitB, pKB, kLen, pVB, vLen);
    if( rc!=SQLITE_OK ) break;
    splitBytes += kLen + vLen;

    /* Feed key bytes into rolling hash */
    int byteIdx;
    for(byteIdx = 0; byteIdx < kLen; byteIdx++){
      prollyRollingHashUpdate(&rh, pKB[byteIdx]);
    }

    /* Check for split boundary */
    if( splitBytes >= PROLLY_CHUNK_MIN
     && ei < pBuilder->nItems - 1
     && pResult->nLeafSplits < MAX_LEAF_SPLITS - 1 ){
      int atBound = prollyRollingHashAtBoundary(&rh,
                      PROLLY_CHUNK_PATTERN);
      if( atBound || splitBytes >= PROLLY_CHUNK_MAX ){
        rc = builderToChunk(&splitB, pMut->pStore,
                            &pResult->aLeafHash[pResult->nLeafSplits]);
        if( rc!=SQLITE_OK ) break;
        if( kLen > (int)sizeof(pResult->aLeafKeyBuf[0]) ){
          rc = SQLITE_TOOBIG;
          break;
        }
        memcpy(pResult->aLeafKeyBuf[pResult->nLeafSplits], pKB, kLen);
        pResult->aLeafKeyLen[pResult->nLeafSplits] = kLen;
        pResult->nLeafSplits++;
        prollyNodeBuilderReset(&splitB);
        prollyRollingHashReset(&rh);
        splitBytes = 0;
      }
    }
  }

  /* Flush remaining entries as the last split node */
  if( rc==SQLITE_OK && splitB.nItems > 0 ){
    rc = builderToChunk(&splitB, pMut->pStore,
                        &pResult->aLeafHash[pResult->nLeafSplits]);
    if( rc==SQLITE_OK ) pResult->nLeafSplits++;
  }

  prollyNodeBuilderFree(&splitB);
  prollyRollingHashFree(&rh);

  if( rc==SQLITE_OK && pResult->nLeafSplits == 1 ){
    memcpy(&pResult->newHash, &pResult->aLeafHash[0], sizeof(ProllyHash));
  }

  return rc;
}

/*
** Phase 4: Walk up the cursor's path stack from the leaf's parent to
** the root, rebuilding each ancestor node with the new child hashes
** produced by the leaf split.
**
** On return, *pNewHash holds the new root hash for this edit batch.
*/
static int updateAncestors(
  ProllyMutator *pMut,
  ProllyCursor *pCur,
  int leafLevel,
  LeafSplitResult *pSplit,
  ProllyHash *pNewHash
){
  int rc;
  int level;

  memcpy(pNewHash, &pSplit->newHash, sizeof(ProllyHash));

  for(level = leafLevel - 1; level >= 0; level--){
    ProllyNode *pAnc = &pCur->aLevel[level].pEntry->node;
    int childIdx = pCur->aLevel[level].idx;
    ProllyNodeBuilder ancBuilder;

    prollyNodeBuilderInit(&ancBuilder, (u8)(pAnc->level), pMut->flags);

    int k;
    for(k = 0; k < pAnc->nItems; k++){
      const u8 *pK; int nK;
      const u8 *pV; int nV;
      prollyNodeKey(pAnc, k, &pK, &nK);
      if( k == childIdx ){
        if( pSplit->nLeafSplits == 0
         || (pSplit->nLeafSplits == 1 && prollyHashIsEmpty(pNewHash)) ){
          continue;  /* Child deleted — skip */
        }
        if( pSplit->nLeafSplits > 1 && level == leafLevel - 1 ){
          /* Multiple leaf splits: insert all children.
          ** All but the last get their own stored key;
          ** the last reuses the original parent key. */
          int si;
          for(si = 0; si < pSplit->nLeafSplits - 1; si++){
            rc = prollyNodeBuilderAdd(&ancBuilder,
                   pSplit->aLeafKeyBuf[si], pSplit->aLeafKeyLen[si],
                   pSplit->aLeafHash[si].data, PROLLY_HASH_SIZE);
            if( rc!=SQLITE_OK ) break;
          }
          if( rc==SQLITE_OK ){
            rc = prollyNodeBuilderAdd(&ancBuilder, pK, nK,
                   pSplit->aLeafHash[pSplit->nLeafSplits-1].data,
                   PROLLY_HASH_SIZE);
          }
        }else{
          /* Single replacement */
          rc = prollyNodeBuilderAdd(&ancBuilder, pK, nK,
                                    pNewHash->data, PROLLY_HASH_SIZE);
        }
      }else{
        prollyNodeValue(pAnc, k, &pV, &nV);
        rc = prollyNodeBuilderAdd(&ancBuilder, pK, nK, pV, nV);
      }
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&ancBuilder);
        return rc;
      }
    }

    if( ancBuilder.nItems == 0 ){
      /* Ancestor became empty — propagate up */
      memset(pNewHash, 0, sizeof(ProllyHash));
      prollyNodeBuilderFree(&ancBuilder);
      continue;
    }

    rc = builderToChunk(&ancBuilder, pMut->pStore, pNewHash);
    prollyNodeBuilderFree(&ancBuilder);
    if( rc!=SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

/*
** Apply edits to a tree using the Dolt-style cursor-path-stack approach.
**
** Algorithm:
**   1. Iterate edits in sorted order.
**   2. For each edit, seek cursor to the edit key. The cursor's path
**      stack now points from root to the target leaf.
**   3. applyEditsToLeaf: merge the leaf's entries with pending edits.
**   4. handleLeafSplits: serialize the rebuilt leaf, splitting if needed.
**   5. updateAncestors: walk up the path stack, replacing child hashes.
**   6. The final hash is the new root for this batch of edits.
**
** Cost: O(M * (log N + L)) where M = edits, L = avg leaf size.
** Unchanged subtrees are never touched — only the path from root
** to the edited leaf is rewritten.
*/
static int applyEdits(
  ProllyMutator *pMut
){
  ProllyCursor cur;
  ProllyMutMapIter iter;
  int rc;
  int seekRes;
  ProllyHash currentRoot;

  memcpy(&currentRoot, &pMut->oldRoot, sizeof(ProllyHash));
  prollyMutMapIterFirst(&iter, pMut->pEdits);

  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);

    /* --- Phase 1: Seek to edit position --- */
    prollyCursorInit(&cur, pMut->pStore, pMut->pCache,
                     &currentRoot, pMut->flags);
    if( pMut->flags & PROLLY_NODE_INTKEY ){
      rc = prollyCursorSeekInt(&cur, pEd->intKey, &seekRes);
    }else{
      rc = prollyCursorSeekBlob(&cur, pEd->pKey, pEd->nKey, &seekRes);
    }
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }

    int leafLevel = cur.nLevel - 1;
    ProllyNode *pLeaf = &cur.aLevel[leafLevel].pEntry->node;

    /* --- Phase 2: Rebuild leaf with edits applied --- */
    ProllyNodeBuilder leafBuilder;
    prollyNodeBuilderInit(&leafBuilder, 0, pMut->flags);

    rc = applyEditsToLeaf(pMut, pLeaf, &cur, leafLevel, &iter, &leafBuilder);
    if( rc!=SQLITE_OK ){
      prollyNodeBuilderFree(&leafBuilder);
      prollyCursorClose(&cur);
      return rc;
    }

    /* --- Phase 3: Handle leaf splits --- */
    LeafSplitResult splitResult;
    rc = handleLeafSplits(pMut, &leafBuilder, &splitResult);
    prollyNodeBuilderFree(&leafBuilder);
    if( rc!=SQLITE_OK ){
      sqlite3_free(splitResult.aLeafKeyBuf);
      prollyCursorClose(&cur);
      return rc;
    }

    /* --- Phase 4: Walk up ancestors --- */
    /* For single-leaf trees (leafLevel==0), the leaf IS the root. */
    if( leafLevel == 0 ){
      memcpy(&currentRoot, &splitResult.newHash, sizeof(ProllyHash));
    }else{
      ProllyHash newRoot;
      rc = updateAncestors(pMut, &cur, leafLevel, &splitResult, &newRoot);
      if( rc!=SQLITE_OK ){
        sqlite3_free(splitResult.aLeafKeyBuf);
        prollyCursorClose(&cur);
        return rc;
      }
      memcpy(&currentRoot, &newRoot, sizeof(ProllyHash));
    }

    sqlite3_free(splitResult.aLeafKeyBuf);
    prollyCursorClose(&cur);
  }

  memcpy(&pMut->newRoot, &currentRoot, sizeof(ProllyHash));
  return SQLITE_OK;
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

  /* Case 3: hybrid flush — pick strategy based on edit count vs tree size.
  **
  ** applyEdits (cursor-path): O(M * log N) — great when M << N
  ** mergeWalk (full merge):   O(N + M)     — better when M ≈ N
  **
  ** Estimate N by loading the root node: for a tree of height H with
  ** root having R children, N ≈ R * (avg_entries_per_subtree).
  ** A rough estimate using ~50 entries per leaf and branching factor ~50:
  **   height 0 (leaf root): N = root.nItems
  **   height 1: N ≈ root.nItems * 50
  **   height 2: N ≈ root.nItems * 2500
  */
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

    if( M > threshold ){
      return mergeWalk(pMut);
    }else{
      return applyEdits(pMut);
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
