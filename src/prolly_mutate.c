
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

#define PROLLY_EST_ENTRIES_PER_LEAF 50

static int compareKeys(
  const u8 *pKey1, int nKey1,
  const u8 *pKey2, int nKey2
){
  int n = nKey1 < nKey2 ? nKey1 : nKey2;
  int c = memcmp(pKey1, pKey2, n);
  if( c != 0 ) return c;
  if( nKey1 < nKey2 ) return -1;
  if( nKey1 > nKey2 ) return 1;
  return 0;
}

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
      rc = prollyChunkerAdd(&chunker, pEntry->pKey, pEntry->nKey,
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

/* mergeWalk removed: streamingMerge now handles all flush shapes. */

static int subtreeHasEdits(
  ProllyMutMapIter *pIter,
  const u8 *pBoundKey, int nBoundKey
){
  ProllyMutMapEntry *pEd;
  int cmp;
  if( !prollyMutMapIterValid(pIter) ) return 0;
  pEd = prollyMutMapIterEntry(pIter);
  cmp = compareKeys(pEd->pKey, pEd->nKey, pBoundKey, nBoundKey);
  return (cmp <= 0);
}

static int chunkerLevelsBelowEmpty(
  const ProllyChunker *pChunker,
  int level
){
  int i;
  for( i = 0; i < level && i < pChunker->nLevels; i++ ){
    if( pChunker->aLevel[i].builder.nItems > 0 ){
      return 0;
    }
  }
  return 1;
}

static int mergeLeaf(
  ProllyMutator *pMut,
  ProllyNode *pLeaf,
  ProllyChunker *pCh,
  ProllyMutMapIter *pIter,
  int isLast
){
  int rc = SQLITE_OK;
  int j;
  u8 flags = pMut->flags;

  for( j = 0; j < pLeaf->nItems; ){
    int haveEdit = prollyMutMapIterValid(pIter);
    ProllyMutMapEntry *pEd = haveEdit ? prollyMutMapIterEntry(pIter) : 0;

    const u8 *pCurKey; int nCurKey;
    int cmp;

    prollyNodeKey(pLeaf, j, &pCurKey, &nCurKey);

    if( !haveEdit ){

      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
      continue;
    }


    {
      const u8 *pLastKey; int nLastKey;
      int pastLeaf;
      prollyNodeKey(pLeaf, pLeaf->nItems - 1, &pLastKey, &nLastKey);
      pastLeaf = compareKeys(pEd->pKey, pEd->nKey, pLastKey, nLastKey);
      if( pastLeaf > 0 ){

        const u8 *pVal; int nVal;
        prollyNodeValue(pLeaf, j, &pVal, &nVal);
        rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }
    }

    cmp = compareKeys(pCurKey, nCurKey, pEd->pKey, pEd->nKey);
    if( cmp < 0 ){

      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else if( cmp == 0 ){

      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAdd(pCh, pEd->pKey, pEd->nKey, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      j++;
      prollyMutMapIterNext(pIter);
    }else{

      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAdd(pCh, pEd->pKey, pEd->nKey, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      prollyMutMapIterNext(pIter);
    }
  }

  /* Drain trailing edits past the leaf's last key when this is the
  ** rightmost leaf of an isLast subtree. Feeding them at level 0
  ** here lets them chunk up alongside the leaf's items, instead of
  ** the previous "trailing-append at level 0 + propagate up through
  ** every intermediate level" path that grew tree depth by 1 per
  ** flush in the worst case. */
  if( isLast ){
    while( prollyMutMapIterValid(pIter) ){
      ProllyMutMapEntry *pEd = prollyMutMapIterEntry(pIter);
      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAdd(pCh, pEd->pKey, pEd->nKey, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) return rc;
      }
      prollyMutMapIterNext(pIter);
    }
  }

  return SQLITE_OK;
}

static int streamingMergeNode(
  ProllyMutator *pMut,
  const ProllyNode *pNode,
  ProllyChunker *pChunker,
  ProllyMutMapIter *pIter,
  int isLast
){
  ProllyCache *pCache = pMut->pCache;
  int rc = SQLITE_OK;
  int i;

  for( i = 0; i < pNode->nItems; i++ ){
    const u8 *pBoundKey; int nBoundKey;
    const u8 *pChildVal; int nChildVal;
    int childIsLast;
    int forceDescend;

    prollyNodeKey(pNode, i, &pBoundKey, &nBoundKey);
    prollyNodeValue(pNode, i, &pChildVal, &nChildVal);

    childIsLast = isLast && (i == pNode->nItems - 1);

    /* The rightmost child of an isLast subtree must absorb any
    ** trailing edits past pBoundKey. If we splice it instead, those
    ** edits get appended at chunker level 0 and propagate up through
    ** every intermediate level as a column of single-entry chunks —
    ** which is what produced the depth pathology that hit MAX_DEPTH
    ** at ~1900 single-row inserts. */
    forceDescend = childIsLast && prollyMutMapIterValid(pIter);

    if( !forceDescend
     && !subtreeHasEdits(pIter, pBoundKey, nBoundKey)
     && chunkerLevelsBelowEmpty(pChunker, pNode->level) ){
      rc = prollyChunkerAddAtLevel(pChunker, pNode->level,
                                    pBoundKey, nBoundKey,
                                    pChildVal, nChildVal);
      if( rc!=SQLITE_OK ) return rc;
    }else{
      ProllyHash childHash;
      ProllyCacheEntry *pChildEntry;
      u8 *pChildData = 0;
      int nChildData = 0;

      assert( nChildVal == PROLLY_HASH_SIZE );
      memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
      pChildEntry = prollyCacheGet(pCache, &childHash);
      if( !pChildEntry ){
        rc = chunkStoreGet(pMut->pStore, &childHash, &pChildData, &nChildData);
        if( rc!=SQLITE_OK ) return rc;
        pChildEntry = prollyCachePut(pCache, &childHash, pChildData, nChildData, &rc);
        sqlite3_free(pChildData);
        if( !pChildEntry ) return rc;
      }

      if( pChildEntry->node.level == 0 ){
        rc = mergeLeaf(pMut, &pChildEntry->node, pChunker, pIter, childIsLast);
      }else{
        rc = streamingMergeNode(pMut, &pChildEntry->node, pChunker, pIter, childIsLast);
      }
      prollyCacheRelease(pCache, pChildEntry);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

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


  rc = chunkStoreGet(pMut->pStore, &pMut->oldRoot, &pRootData, &nRootData);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(&rootNode, pRootData, nRootData);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pRootData);
    return rc;
  }


  prollyMutMapIterFirst(&iter, pMut->pEdits);
  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pRootData);
    return rc;
  }

  /* isLast=1 marks the root as the rightmost subtree at this level
  ** of recursion, propagating down through the rightmost child of
  ** each visited node. mergeLeaf at the bottom drains any trailing
  ** edits past the rightmost leaf's last key.
  **
  ** A root that is itself a leaf (level == 0) is just the rightmost
  ** leaf at the top of the tree — drain edits into it via mergeLeaf
  ** directly, no need for streamingMergeNode's child-iteration. */
  if( rootNode.level == 0 ){
    rc = mergeLeaf(pMut, &rootNode, &chunker, &iter, /*isLast=*/1);
  }else{
    rc = streamingMergeNode(pMut, &rootNode, &chunker, &iter, /*isLast=*/1);
  }
  if( rc!=SQLITE_OK ) goto streaming_cleanup;

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

streaming_cleanup:
  prollyChunkerFree(&chunker);
  sqlite3_free(pRootData);
  return rc;
}


/* streamingMerge is now the only flush strategy. It walks the tree once,
** splicing unchanged subtrees by re-emitting their hash references at the
** matched level (Dolt's chunker.advanceTo equivalent), and drains trailing
** edits into the rightmost leaf to avoid the depth pathology that the
** earlier mergeWalk fallback was guarding against. With those correctness
** properties in place there is no shape on which a full-rebuild walk would
** be required for correctness, so the per-flush strategy choice (which had
** been pessimizing toward mergeWalk for any non-trivial edit ratio) is
** dropped in favor of the unified path — matching Dolt's "one algorithm,
** no INTKEY/non-INTKEY split" property. */
int prollyMutateFlush(ProllyMutator *pMut){
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  return streamingMerge(pMut);
}

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


  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }


  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;


  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

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


  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }


  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;


  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

#endif
