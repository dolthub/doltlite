
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include "prolly_check.h"
#include "prolly_xxhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PROLLY_EST_ENTRIES_PER_LEAF 50

static int subtreeCountByHash(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pHash,
  u64 *pCount
){
  ProllyCacheEntry *pEntry = 0;
  u8 *pData = 0;
  int nData = 0;
  int rc = SQLITE_OK;
  int i;
  u64 sum = 0;

  pEntry = prollyCacheGet(pCache, pHash);
  if( !pEntry ){
    rc = chunkStoreGet(pStore, pHash, &pData, &nData);
    if( rc!=SQLITE_OK ) return rc;
    pEntry = prollyCachePut(pCache, pHash, pData, nData, &rc);
    sqlite3_free(pData);
    if( !pEntry ) return rc;
  }

  if( pEntry->node.level==0 ){
    sum = (u64)pEntry->node.nItems;
  }else if( prollyNodeHasSubtreeCounts(&pEntry->node) ){
    for( i=0; i<(int)pEntry->node.nItems; i++ ){
      sum += prollyNodeChildSubtreeCount(&pEntry->node, i);
    }
  }else{
    for( i=0; i<(int)pEntry->node.nItems; i++ ){
      ProllyHash childHash;
      u64 childCount = 0;
      prollyNodeChildHash(&pEntry->node, i, &childHash);
      rc = subtreeCountByHash(pStore, pCache, &childHash, &childCount);
      if( rc!=SQLITE_OK ){
        prollyCacheRelease(pCache, pEntry);
        return rc;
      }
      sum += childCount;
    }
  }

  prollyCacheRelease(pCache, pEntry);
  *pCount = sum;
  return SQLITE_OK;
}

static int parentChildSubtreeCount(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyNode *pParent,
  int i,
  u64 *pCount
){
  if( prollyNodeHasSubtreeCounts(pParent) ){
    *pCount = prollyNodeChildSubtreeCount(pParent, i);
    return SQLITE_OK;
  }else{
    ProllyHash childHash;
    prollyNodeChildHash(pParent, i, &childHash);
    return subtreeCountByHash(pStore, pCache, &childHash, pCount);
  }
}

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

  rc = prollyChunkerInitWithCache(&chunker, pMut->pStore, pMut->pCache,
                                  pMut->flags);
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

#ifdef DOLTLITE_PROLLY_CHECK
  if( pNode->level == 0 ){
    fprintf(stderr,
            "doltlite: E8 invariant: streamingMergeNode called on a "
            "level-0 (leaf) node; expected internal\n");
    abort();
  }
#endif

  for( i = 0; i < pNode->nItems; i++ ){
    const u8 *pBoundKey; int nBoundKey;
    const u8 *pChildVal; int nChildVal;
    int childIsLast;
    int forceDescend;

    prollyNodeKey(pNode, i, &pBoundKey, &nBoundKey);
    prollyNodeValue(pNode, i, &pChildVal, &nChildVal);

    childIsLast = isLast && (i == pNode->nItems - 1);

    forceDescend = childIsLast && prollyMutMapIterValid(pIter);

    if( !forceDescend
     && !subtreeHasEdits(pIter, pBoundKey, nBoundKey)
     && chunkerLevelsBelowEmpty(pChunker, pNode->level) ){
      u64 childCount = 0;
      rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                   pNode, i, &childCount);
      if( rc!=SQLITE_OK ) return rc;
      rc = prollyChunkerAddAtLevelWithCount(pChunker, pNode->level,
                                            pBoundKey, nBoundKey,
                                            pChildVal, nChildVal,
                                            childCount);
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

#ifdef DOLTLITE_PROLLY_CHECK
      if( pChildEntry->node.level != pNode->level - 1 ){
        fprintf(stderr,
                "doltlite: E8 invariant: parent level=%d but child level=%d "
                "(expected %d)\n",
                (int)pNode->level, (int)pChildEntry->node.level,
                (int)pNode->level - 1);
        abort();
      }
#endif
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
  ProllyNode *pRootNode = &rootNode;
  ProllyCacheEntry *pRootEntry = 0;
  ProllyCache *pCache = pMut->pCache;

  pRootEntry = pCache ? prollyCacheGet(pCache, &pMut->oldRoot) : 0;
  if( pRootEntry ){
    pRootNode = &pRootEntry->node;
  }else{
    rc = chunkStoreGet(pMut->pStore, &pMut->oldRoot, &pRootData, &nRootData);
    if( rc!=SQLITE_OK ) return rc;
    rc = prollyNodeParse(&rootNode, pRootData, nRootData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pRootData);
      return rc;
    }
  }

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  rc = prollyChunkerInitWithCache(&chunker, pMut->pStore, pMut->pCache,
                                  pMut->flags);
  if( rc!=SQLITE_OK ){
    if( pRootEntry ) prollyCacheRelease(pCache, pRootEntry);
    sqlite3_free(pRootData);
    return rc;
  }

  if( pRootNode->level == 0 ){
    rc = mergeLeaf(pMut, pRootNode, &chunker, &iter, 1);
  }else{
    rc = streamingMergeNode(pMut, pRootNode, &chunker, &iter, 1);
  }
  if( rc!=SQLITE_OK ) goto streaming_cleanup;

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

streaming_cleanup:
  if( pRootEntry ) prollyCacheRelease(pCache, pRootEntry);
  prollyChunkerFree(&chunker);
  sqlite3_free(pRootData);
  return rc;
}

static void nodeAppendState(const ProllyNode *pNode,
                            const u8 *pKey, int nKey,
                            int nVal,
                            int *pEndsAtBoundary,
                            int *pWouldSplit){
  int i;
  int nBytes = 0;
  int thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
  int endsAtBoundary = 0;
  int wouldSplit = 0;

  for(i=0; i<(int)pNode->nItems; i++){
    const u8 *pK, *pV;
    int nK, nV;
    prollyNodeKey(pNode, i, &pK, &nK);
    prollyNodeValue(pNode, i, &pV, &nV);
    (void)pK; (void)pV;
    nBytes += PROLLY_NODE_ENTRY_BYTES(pNode->level, nK, nV);
  }
  if( pNode->nItems>0 && nBytes >= PROLLY_CHUNK_MAX ){
    endsAtBoundary = 1;
  }else if( pNode->nItems>0 && nBytes >= PROLLY_CHUNK_MIN ){
    const u8 *pLastKey, *pLastVal;
    int nLastKey, nLastVal;
    u32 h;
    prollyNodeKey(pNode, (int)pNode->nItems - 1, &pLastKey, &nLastKey);
    prollyNodeValue(pNode, (int)pNode->nItems - 1, &pLastVal, &nLastVal);
    (void)pLastVal;
    h = prollyXXH32(pLastKey, nLastKey, (u32)pNode->level);
    endsAtBoundary = prollyWeibullCheck((u32)nBytes,
                                        (u32)PROLLY_NODE_ENTRY_BYTES(
                                          pNode->level, nLastKey, nLastVal),
                                        h);
  }

  nBytes += thisSize;
  if( nBytes >= PROLLY_CHUNK_MAX ){
    wouldSplit = 1;
  }else if( nBytes >= PROLLY_CHUNK_MIN ){
    u32 h = prollyXXH32(pKey, nKey, 0);
    wouldSplit = prollyWeibullCheck((u32)nBytes, (u32)thisSize, h);
  }

  *pEndsAtBoundary = endsAtBoundary;
  *pWouldSplit = wouldSplit;
}

static int appendWouldSplitNode(const ProllyNode *pNode,
                                const u8 *pKey, int nKey,
                                int nVal){
  int i;
  int nBytes = 0;
  int thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
  for(i=0; i<(int)pNode->nItems; i++){
    const u8 *pK, *pV;
    int nK, nV;
    prollyNodeKey(pNode, i, &pK, &nK);
    prollyNodeValue(pNode, i, &pV, &nV);
    (void)pK; (void)pV;
    nBytes += PROLLY_NODE_ENTRY_BYTES(pNode->level, nK, nV);
  }
  nBytes += thisSize;
  if( nBytes >= PROLLY_CHUNK_MAX ) return 1;
  if( nBytes >= PROLLY_CHUNK_MIN ){
    u32 h = prollyXXH32(pKey, nKey, (u32)pNode->level);
    return prollyWeibullCheck((u32)nBytes, (u32)thisSize, h);
  }
  return 0;
}

static int nodeEndsAtChunkBoundary(const ProllyNode *pNode){
  const u8 *pKey, *pVal;
  int nKey, nVal;
  int nBytes = 0;
  int i;

  if( pNode->nItems==0 ) return 0;
  for(i=0; i<(int)pNode->nItems; i++){
    prollyNodeKey(pNode, i, &pKey, &nKey);
    prollyNodeValue(pNode, i, &pVal, &nVal);
    (void)pVal;
    nBytes += PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
  }
  if( nBytes >= PROLLY_CHUNK_MAX ) return 1;
  if( nBytes >= PROLLY_CHUNK_MIN ){
    u32 h;
    prollyNodeKey(pNode, (int)pNode->nItems - 1, &pKey, &nKey);
    prollyNodeValue(pNode, (int)pNode->nItems - 1, &pVal, &nVal);
    h = prollyXXH32(pKey, nKey, (u32)pNode->level);
    return prollyWeibullCheck((u32)nBytes,
                              (u32)PROLLY_NODE_ENTRY_BYTES(pNode->level,
                                                           nKey, nVal), h);
  }
  return 0;
}

static int nodeHasSameChunkShape(const ProllyNode *pNode,
                                 int requireFinalBoundary){
  const u8 *pKey, *pVal;
  int nKey, nVal;
  int nBytes = 0;
  int i;

  if( pNode->nItems==0 ) return 0;
  for(i=0; i<(int)pNode->nItems; i++){
    int isBoundary = 0;
    int thisSize;

    prollyNodeKey(pNode, i, &pKey, &nKey);
    prollyNodeValue(pNode, i, &pVal, &nVal);
    (void)pVal;
    thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
    nBytes += thisSize;
    if( nBytes >= PROLLY_CHUNK_MAX ){
      isBoundary = 1;
    }else if( nBytes >= PROLLY_CHUNK_MIN ){
      u32 h = prollyXXH32(pKey, nKey, (u32)pNode->level);
      isBoundary = prollyWeibullCheck((u32)nBytes, (u32)thisSize, h);
    }

    if( i<(int)pNode->nItems - 1 && isBoundary ){
      return 0;
    }
    if( i==(int)pNode->nItems - 1 && requireFinalBoundary && !isBoundary ){
      return 0;
    }
  }
  return 1;
}

static int appendNodeEntryToBuilder(
  ProllyNodeBuilder *pBuilder,
  const ProllyNode *pNode,
  int i,
  u64 subtreeCount
){
  const u8 *pKey, *pVal;
  int nKey, nVal;
  prollyNodeKey(pNode, i, &pKey, &nKey);
  prollyNodeValue(pNode, i, &pVal, &nVal);
  if( pNode->level==0 ){
    return prollyNodeBuilderAdd(pBuilder, pKey, nKey, pVal, nVal);
  }
  return prollyNodeBuilderAddWithCount(pBuilder, pKey, nKey, pVal, nVal,
                                       subtreeCount);
}

static int writeBuilderNode(ChunkStore *pStore, ProllyNodeBuilder *pBuilder,
                            ProllyHash *pHash){
  u8 *pData = 0;
  int nData = 0;
  int rc = prollyNodeBuilderFinish(pBuilder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(pStore, pData, nData, pHash);
  sqlite3_free(pData);
  return rc;
}

static int finishAndWriteBuilderNode(ChunkStore *pStore,
                                     ProllyNodeBuilder *pBuilder,
                                     int requireBoundary,
                                     ProllyHash *pHash){
  u8 *pData = 0;
  int nData = 0;
  int rc = prollyNodeBuilderFinish(pBuilder, &pData, &nData);
  ProllyNode node;

  if( rc!=SQLITE_OK ) return rc;
  rc = prollyNodeParse(&node, pData, nData);
  if( rc==SQLITE_OK && !nodeHasSameChunkShape(&node, requireBoundary) ){
    rc = SQLITE_NOTFOUND;
  }
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(pStore, pData, nData, pHash);
  }
  sqlite3_free(pData);
  return rc;
}

static int cursorLeafIsRightmost(ProllyCursor *pCur){
  int i;
  for(i=0; i<pCur->iLevel; i++){
    ProllyNode *pNode = &pCur->aLevel[i].pEntry->node;
    if( pCur->aLevel[i].idx != (int)pNode->nItems - 1 ){
      return 0;
    }
  }
  return 1;
}

static int rewriteAncestorSpine(
  ProllyMutator *pMut,
  ProllyCursor *pCur,
  int level,
  ProllyHash *pChildHash,
  int countDelta
){
  int rc = SQLITE_OK;

  while( level>0 ){
    ProllyNode *pNode;
    ProllyNodeBuilder b;
    ProllyHash parentHash;
    int idx;
    int i;

    level--;
    pNode = &pCur->aLevel[level].pEntry->node;
    idx = pCur->aLevel[level].idx;
    prollyNodeBuilderInit(&b, (u8)pNode->level, pMut->flags);
    for(i=0; i<(int)pNode->nItems; i++){
      u64 cnt = 0;
      rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                   pNode, i, &cnt);
      if( rc==SQLITE_OK ){
        if( i==idx ){
          const u8 *pKey;
          int nKey;
          if( countDelta<0 && cnt==0 ){
            rc = SQLITE_CORRUPT;
          }else{
            prollyNodeKey(pNode, i, &pKey, &nKey);
            rc = prollyNodeBuilderAddWithCount(
                &b, pKey, nKey, pChildHash->data, PROLLY_HASH_SIZE,
                countDelta<0 ? cnt - 1 : cnt + 1);
          }
        }else{
          rc = appendNodeEntryToBuilder(&b, pNode, i, cnt);
        }
      }
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        return rc;
      }
    }
    rc = writeBuilderNode(pMut->pStore, &b, &parentHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ) return rc;
    *pChildHash = parentHash;
  }

  return SQLITE_OK;
}

/* Replace one existing row when node boundaries cannot change. */
static int tryReplaceSingleSameSize(ProllyMutator *pMut){
  ProllyMutMapEntry *pEdit;
  ProllyCursor cur;
  ProllyHash childHash;
  i64 iKey;
  int rc;
  int res = 0;
  int level;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=1 ) return SQLITE_NOTFOUND;

  pEdit = &pMut->pEdits->aEntries[0];
  if( pEdit->op!=PROLLY_EDIT_INSERT ){
    return SQLITE_NOTFOUND;
  }
  if( (pMut->flags & PROLLY_NODE_INTKEY) && pEdit->nKey!=8 ){
    return SQLITE_NOTFOUND;
  }

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache, &pMut->oldRoot,
                   pMut->flags);
  if( pMut->flags & PROLLY_NODE_INTKEY ){
    iKey = prollyMutMapEntryIntKey(pEdit);
    rc = prollyCursorSeekInt(&cur, iKey, &res);
  }else{
    rc = prollyCursorSeekBlob(&cur, pEdit->pKey, pEdit->nKey, &res);
  }
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 || cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  level = cur.iLevel;
  {
    ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
    int idx = cur.aLevel[level].idx;
    const u8 *pOldVal;
    int nOldVal;
    ProllyNodeBuilder b;
    int i;

    prollyNodeValue(pLeaf, idx, &pOldVal, &nOldVal);
    if( nOldVal!=pEdit->nVal ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }

    prollyNodeBuilderInit(&b, 0, pMut->flags);
    for(i=0; i<(int)pLeaf->nItems; i++){
      if( i==idx ){
        rc = prollyNodeBuilderAdd(&b, pEdit->pKey, pEdit->nKey,
                                  pEdit->pVal, pEdit->nVal);
      }else{
        rc = appendNodeEntryToBuilder(&b, pLeaf, i, 0);
      }
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    rc = writeBuilderNode(pMut->pStore, &b, &childHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }

  while( level>0 ){
    ProllyNode *pNode;
    ProllyNodeBuilder b;
    ProllyHash parentHash;
    int idx;
    int i;

    level--;
    pNode = &cur.aLevel[level].pEntry->node;
    idx = cur.aLevel[level].idx;
    prollyNodeBuilderInit(&b, (u8)pNode->level, pMut->flags);
    for(i=0; i<(int)pNode->nItems; i++){
      u64 cnt = 0;
      rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                   pNode, i, &cnt);
      if( rc==SQLITE_OK ){
        if( i==idx ){
          const u8 *pKey;
          int nKey;
          prollyNodeKey(pNode, i, &pKey, &nKey);
          rc = prollyNodeBuilderAddWithCount(
              &b, pKey, nKey, childHash.data, PROLLY_HASH_SIZE, cnt);
        }else{
          rc = appendNodeEntryToBuilder(&b, pNode, i, cnt);
        }
      }
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    rc = writeBuilderNode(pMut->pStore, &b, &parentHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
    childHash = parentHash;
  }

  pMut->newRoot = childHash;
  prollyCursorClose(&cur);
  return SQLITE_OK;
}

static int tryDeleteSingleNoRechunk(ProllyMutator *pMut){
  ProllyMutMapEntry *pEdit;
  ProllyCursor cur;
  ProllyHash childHash;
  i64 iKey;
  int rc;
  int res = 0;
  int level;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=1 ) return SQLITE_NOTFOUND;

  pEdit = &pMut->pEdits->aEntries[0];
  if( pEdit->op!=PROLLY_EDIT_DELETE ){
    return SQLITE_NOTFOUND;
  }
  if( (pMut->flags & PROLLY_NODE_INTKEY) && pEdit->nKey!=8 ){
    return SQLITE_NOTFOUND;
  }

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache, &pMut->oldRoot,
                   pMut->flags);
  if( pMut->flags & PROLLY_NODE_INTKEY ){
    iKey = prollyMutMapEntryIntKey(pEdit);
    rc = prollyCursorSeekInt(&cur, iKey, &res);
  }else{
    rc = prollyCursorSeekBlob(&cur, pEdit->pKey, pEdit->nKey, &res);
  }
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 || cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  level = cur.iLevel;
  {
    ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
    int idx = cur.aLevel[level].idx;
    ProllyNodeBuilder b;
    int requireBoundary;
    int i;

    if( pLeaf->nItems<=1 || idx==(int)pLeaf->nItems - 1 ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }

    prollyNodeBuilderInit(&b, 0, pMut->flags);
    for(i=0; i<(int)pLeaf->nItems; i++){
      if( i==idx ) continue;
      rc = appendNodeEntryToBuilder(&b, pLeaf, i, 0);
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    requireBoundary = !cursorLeafIsRightmost(&cur);
    rc = finishAndWriteBuilderNode(pMut->pStore, &b, requireBoundary,
                                   &childHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }

  rc = rewriteAncestorSpine(pMut, &cur, level, &childHash, -1);
  if( rc==SQLITE_OK ){
    pMut->newRoot = childHash;
  }
  prollyCursorClose(&cur);
  return rc;
}

static int tryInsertSingleNoRechunk(ProllyMutator *pMut){
  ProllyMutMapEntry *pEdit;
  ProllyCursor cur;
  ProllyHash childHash;
  i64 iKey = 0;
  int rc;
  int res = 0;
  int level;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=1 ) return SQLITE_NOTFOUND;

  pEdit = &pMut->pEdits->aEntries[0];
  if( pEdit->op!=PROLLY_EDIT_INSERT ){
    return SQLITE_NOTFOUND;
  }
  if( (pMut->flags & PROLLY_NODE_INTKEY) && pEdit->nKey!=8 ){
    return SQLITE_NOTFOUND;
  }

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache, &pMut->oldRoot,
                   pMut->flags);
  if( pMut->flags & PROLLY_NODE_INTKEY ){
    iKey = prollyMutMapEntryIntKey(pEdit);
    rc = prollyCursorSeekInt(&cur, iKey, &res);
  }else{
    rc = prollyCursorSeekBlob(&cur, pEdit->pKey, pEdit->nKey, &res);
  }
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res==0 || cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  if( pMut->flags & PROLLY_NODE_INTKEY ){
    if( iKey>=prollyCursorIntKey(&cur) ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }
  }else{
    const u8 *pCurKey;
    int nCurKey;
    prollyCursorKey(&cur, &pCurKey, &nCurKey);
    if( compareKeys(pEdit->pKey, pEdit->nKey, pCurKey, nCurKey)>=0 ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }
  }

  level = cur.iLevel;
  {
    ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
    int idx = cur.aLevel[level].idx;
    ProllyNodeBuilder b;
    int requireBoundary;
    int i;

    if( idx<0 || idx>=(int)pLeaf->nItems ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }

    prollyNodeBuilderInit(&b, 0, pMut->flags);
    for(i=0; i<(int)pLeaf->nItems; i++){
      if( i==idx ){
        rc = prollyNodeBuilderAdd(&b, pEdit->pKey, pEdit->nKey,
                                  pEdit->pVal, pEdit->nVal);
        if( rc!=SQLITE_OK ){
          prollyNodeBuilderFree(&b);
          prollyCursorClose(&cur);
          return rc;
        }
      }
      rc = appendNodeEntryToBuilder(&b, pLeaf, i, 0);
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    requireBoundary = !cursorLeafIsRightmost(&cur);
    rc = finishAndWriteBuilderNode(pMut->pStore, &b, requireBoundary,
                                   &childHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
  }

  rc = rewriteAncestorSpine(pMut, &cur, level, &childHash, 1);
  if( rc==SQLITE_OK ){
    pMut->newRoot = childHash;
  }
  prollyCursorClose(&cur);
  return rc;
}

static int tryAppendSingleIntNoSplit(ProllyMutator *pMut){
  ProllyMutMapEntry *pEdit;
  ProllyCursor cur;
  ProllyHash childHash;
  const u8 *pNewKey;
  int nNewKey;
  i64 iNewKey;
  int rc;
  int res = 0;
  int level;

  if( !(pMut->flags & PROLLY_NODE_INTKEY) ) return SQLITE_NOTFOUND;
  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=1 ) return SQLITE_NOTFOUND;

  pEdit = &pMut->pEdits->aEntries[0];
  if( pEdit->op!=PROLLY_EDIT_INSERT || pEdit->nKey!=8 ){
    return SQLITE_NOTFOUND;
  }

  pNewKey = pEdit->pKey;
  nNewKey = pEdit->nKey;
  iNewKey = prollyMutMapEntryIntKey(pEdit);

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache, &pMut->oldRoot,
                   pMut->flags);
  rc = prollyCursorLast(&cur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( res!=0 || cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }
  if( iNewKey <= prollyCursorIntKey(&cur) ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  level = cur.iLevel;
  {
    int leafEndsAtBoundary = 0;
    int leafWouldSplit = 0;
    nodeAppendState(&cur.aLevel[level].pEntry->node,
                    pNewKey, nNewKey, pEdit->nVal,
                    &leafEndsAtBoundary, &leafWouldSplit);
    if( leafEndsAtBoundary ){
      int j;
      for(j=1; j<level; j++){
        if( nodeEndsAtChunkBoundary(&cur.aLevel[j].pEntry->node) ){
          prollyCursorClose(&cur);
          return SQLITE_NOTFOUND;
        }
      }
      {
        ProllyNodeBuilder b;
        prollyNodeBuilderInit(&b, 0, pMut->flags);
        rc = prollyNodeBuilderAdd(&b, pNewKey, nNewKey,
                                  pEdit->pVal, pEdit->nVal);
        if( rc==SQLITE_OK ){
          rc = writeBuilderNode(pMut->pStore, &b, &childHash);
        }
        prollyNodeBuilderFree(&b);
        if( rc!=SQLITE_OK ){
          prollyCursorClose(&cur);
          return rc;
        }
      }
      if( level==0 ){
        ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
        ProllyNodeBuilder b;
        const u8 *pOldKey;
        int nOldKey;
        prollyNodeKey(pLeaf, (int)pLeaf->nItems - 1, &pOldKey, &nOldKey);
        prollyNodeBuilderInit(&b, 1, pMut->flags);
        rc = prollyNodeBuilderAddWithCount(&b, pOldKey, nOldKey,
            pMut->oldRoot.data, PROLLY_HASH_SIZE, (u64)pLeaf->nItems);
        if( rc==SQLITE_OK ){
          rc = prollyNodeBuilderAddWithCount(&b, pNewKey, nNewKey,
              childHash.data, PROLLY_HASH_SIZE, 1);
        }
        if( rc!=SQLITE_OK ){
          prollyNodeBuilderFree(&b);
          prollyCursorClose(&cur);
          return rc;
        }
        rc = writeBuilderNode(pMut->pStore, &b, &childHash);
        prollyNodeBuilderFree(&b);
        if( rc!=SQLITE_OK ){
          prollyCursorClose(&cur);
          return rc;
        }
        pMut->newRoot = childHash;
        prollyCursorClose(&cur);
        return SQLITE_OK;
      }else{
        ProllyNode *pParent = &cur.aLevel[level - 1].pEntry->node;
        ProllyNodeBuilder b;
        ProllyHash parentHash;
        int i;
        if( appendWouldSplitNode(pParent, pNewKey, nNewKey, PROLLY_HASH_SIZE) ){
          prollyCursorClose(&cur);
          return SQLITE_NOTFOUND;
        }
        prollyNodeBuilderInit(&b, (u8)pParent->level, pMut->flags);
        for(i=0; i<(int)pParent->nItems; i++){
          u64 cnt = 0;
          rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                       pParent, i, &cnt);
          if( rc==SQLITE_OK ){
            rc = appendNodeEntryToBuilder(&b, pParent, i, cnt);
          }
          if( rc!=SQLITE_OK ){
            prollyNodeBuilderFree(&b);
            prollyCursorClose(&cur);
            return rc;
          }
        }
        rc = prollyNodeBuilderAddWithCount(&b, pNewKey, nNewKey,
                                           childHash.data, PROLLY_HASH_SIZE, 1);
        if( rc==SQLITE_OK ){
          rc = writeBuilderNode(pMut->pStore, &b, &parentHash);
        }
        prollyNodeBuilderFree(&b);
        if( rc!=SQLITE_OK ){
          prollyCursorClose(&cur);
          return rc;
        }
        childHash = parentHash;
        level--;
      }
    }else{
      if( leafWouldSplit ){
        prollyCursorClose(&cur);
        return SQLITE_NOTFOUND;
      }
      {
        ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
        ProllyNodeBuilder b;
        int i;
        prollyNodeBuilderInit(&b, 0, pMut->flags);
        for(i=0; i<(int)pLeaf->nItems; i++){
          rc = appendNodeEntryToBuilder(&b, pLeaf, i, 0);
          if( rc!=SQLITE_OK ){
            prollyNodeBuilderFree(&b);
            prollyCursorClose(&cur);
            return rc;
          }
        }
        rc = prollyNodeBuilderAdd(&b, pNewKey, nNewKey,
                                  pEdit->pVal, pEdit->nVal);
        if( rc==SQLITE_OK ){
          rc = writeBuilderNode(pMut->pStore, &b, &childHash);
        }
        prollyNodeBuilderFree(&b);
        if( rc!=SQLITE_OK ){
          prollyCursorClose(&cur);
          return rc;
        }
      }
    }
  }

  while( level>0 ){
    ProllyNode *pNode;
    ProllyNodeBuilder b;
    ProllyHash parentHash;
    int idx;
    int i;

    level--;
    pNode = &cur.aLevel[level].pEntry->node;
    idx = cur.aLevel[level].idx;
    prollyNodeBuilderInit(&b, (u8)pNode->level, pMut->flags);
    for(i=0; i<(int)pNode->nItems; i++){
      u64 cnt = 0;
      if( i==idx ){
        rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                     pNode, i, &cnt);
        if( rc==SQLITE_OK ){
          rc = prollyNodeBuilderAddWithCount(
              &b, pNewKey, nNewKey, childHash.data, PROLLY_HASH_SIZE,
              cnt + 1);
        }
      }else{
        rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                     pNode, i, &cnt);
        if( rc==SQLITE_OK ){
          rc = appendNodeEntryToBuilder(&b, pNode, i, cnt);
        }
      }
      if( rc!=SQLITE_OK ){
        prollyNodeBuilderFree(&b);
        prollyCursorClose(&cur);
        return rc;
      }
    }
    rc = writeBuilderNode(pMut->pStore, &b, &parentHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }
    childHash = parentHash;
  }

  pMut->newRoot = childHash;
  prollyCursorClose(&cur);
  return SQLITE_OK;
}

int prollyMutateFlush(ProllyMutator *pMut){
  int rc;

  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    rc = buildFromEdits(pMut);
  }else{
    rc = tryReplaceSingleSameSize(pMut);
    if( rc==SQLITE_NOTFOUND ){
      rc = tryAppendSingleIntNoSplit(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = tryDeleteSingleNoRechunk(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = tryInsertSingleNoRechunk(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = streamingMerge(pMut);
    }
  }

#ifdef DOLTLITE_PROLLY_CHECK
  if( rc==SQLITE_OK ){
    if( !prollyHashIsEmpty(&pMut->oldRoot)
     && prollyHashIsEmpty(&pMut->newRoot) ){
      ProllyMutMapIter it;
      int hasInsert = 0;
      prollyMutMapIterFirst(&it, pMut->pEdits);
      while( prollyMutMapIterValid(&it) ){
        ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&it);
        if( pEntry->op==PROLLY_EDIT_INSERT ){
          hasInsert = 1;
          break;
        }
        prollyMutMapIterNext(&it);
      }
      if( hasInsert ){
        fprintf(stderr,
                "doltlite: prolly mutate produced empty root from non-empty"
                " input with INSERT in edit set (S5)\n");
        sqlite3_log(SQLITE_CORRUPT,
                    "prolly mutate empty-root regression: oldRoot non-empty,"
                    " edits include INSERT, newRoot is empty");
        abort();
      }
    }

    {
      char *zErr = 0;
      int crc = prollyCheckTree(pMut->pStore, &pMut->newRoot,
                                pMut->flags, &zErr);
      if( crc==SQLITE_CORRUPT ){
        fprintf(stderr, "doltlite: prolly tree invariant violated: %s\n",
                zErr ? zErr : "(no detail)");
        sqlite3_log(SQLITE_CORRUPT,
                    "prolly tree invariant violated after mutate: %s",
                    zErr ? zErr : "(no detail)");
        sqlite3_free(zErr);
        abort();
      }
      sqlite3_free(zErr);
    }
  }
#endif

  return rc;
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
