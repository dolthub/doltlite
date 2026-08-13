
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include "prolly_cursor.h"
#include "prolly_check.h"
#include "prolly_xxhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PROLLY_EST_ENTRIES_PER_LEAF 50

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
    return prollySubtreeCount(pStore, pCache, &childHash, pCount);
  }
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

  rc = prollyMutMapIterFirst(&iter, pMut->pEdits);
  if( rc!=SQLITE_OK ){
    prollyChunkerFree(&chunker);
    return rc;
  }
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      rc = prollyChunkerAddZeroTail(&chunker, pEntry->pKey, pEntry->nKey,
                                    pEntry->pVal, pEntry->nVal,
                                    pEntry->nZeroTail);
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
  cmp = prollyKeyCmp(pEd->pKey, pEd->nKey, pBoundKey, nBoundKey);
  return (cmp <= 0);
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
  const u8 *pLastKey = 0;
  int nLastKey = 0;

  if( pLeaf->nItems>0 ){
    prollyNodeKey(pLeaf, pLeaf->nItems - 1, &pLastKey, &nLastKey);
  }

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
      int pastLeaf = prollyKeyCmp(pEd->pKey, pEd->nKey, pLastKey, nLastKey);
      if( pastLeaf > 0 ){

        const u8 *pVal; int nVal;
        prollyNodeValue(pLeaf, j, &pVal, &nVal);
        rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }
    }

    cmp = prollyKeyCmp(pCurKey, nCurKey, pEd->pKey, pEd->nKey);
    if( cmp < 0 ){

      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else if( cmp == 0 ){

      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAddZeroTail(pCh, pEd->pKey, pEd->nKey,
                                      pEd->pVal, pEd->nVal, pEd->nZeroTail);
        if( rc!=SQLITE_OK ) return rc;
      }
      j++;
      prollyMutMapIterNext(pIter);
    }else{

      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAddZeroTail(pCh, pEd->pKey, pEd->nKey,
                                      pEd->pVal, pEd->nVal, pEd->nZeroTail);
        if( rc!=SQLITE_OK ) return rc;
      }
      prollyMutMapIterNext(pIter);
    }
  }

  if( isLast ){
    while( prollyMutMapIterValid(pIter) ){
      ProllyMutMapEntry *pEd = prollyMutMapIterEntry(pIter);
      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = prollyChunkerAddZeroTail(pCh, pEd->pKey, pEd->nKey,
                                      pEd->pVal, pEd->nVal, pEd->nZeroTail);
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
     && prollyChunkerLevelsBelowEmpty(pChunker, pNode->level) ){
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

      assert( nChildVal == PROLLY_HASH_SIZE );
      memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
      rc = prollyLoadNode(pMut->pStore, pCache, &childHash, &pChildEntry);
      if( rc!=SQLITE_OK ) return rc;

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

  rc = prollyMutMapIterFirst(&iter, pMut->pEdits);
  if( rc==SQLITE_OK ){
    rc = prollyChunkerInitWithCache(&chunker, pMut->pStore, pMut->pCache,
                                    pMut->flags);
  }
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

/* Boundary predicate shared with the chunker (prolly_chunker.c addToLevel): an
** entry of thisSize bytes whose key is pKey closes a chunk when the running
** byte count reaches PROLLY_CHUNK_MAX, or PROLLY_CHUNK_MIN and the Weibull
** roll fires. Seeded with the node level so leaves and internal nodes match
** the shapes the chunker produces. */
static int chunkBoundary(int nBytes, int thisSize,
                         const u8 *pKey, int nKey, u8 level){
  if( nBytes >= PROLLY_CHUNK_MAX ) return 1;
  if( nBytes >= PROLLY_CHUNK_MIN ){
    u32 h = prollyXXH32(pKey, nKey, (u32)level);
    return prollyWeibullCheck((u32)nBytes, (u32)thisSize, h);
  }
  return 0;
}

static int nodeTotalBytes(const ProllyNode *pNode){
  int i;
  int nBytes = 0;
  for(i=0; i<(int)pNode->nItems; i++){
    const u8 *pK, *pV;
    int nK, nV;
    prollyNodeKey(pNode, i, &pK, &nK);
    prollyNodeValue(pNode, i, &pV, &nV);
    (void)pK; (void)pV;
    nBytes += PROLLY_NODE_ENTRY_BYTES(pNode->level, nK, nV);
  }
  return nBytes;
}

static void nodeAppendState(const ProllyNode *pNode,
                            const u8 *pKey, int nKey,
                            int nVal,
                            int *pEndsAtBoundary,
                            int *pWouldSplit){
  int nBytes = nodeTotalBytes(pNode);
  int thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);

  *pEndsAtBoundary = 0;
  if( pNode->nItems>0 ){
    const u8 *pLastKey, *pLastVal;
    int nLastKey, nLastVal;
    prollyNodeKey(pNode, (int)pNode->nItems - 1, &pLastKey, &nLastKey);
    prollyNodeValue(pNode, (int)pNode->nItems - 1, &pLastVal, &nLastVal);
    *pEndsAtBoundary = chunkBoundary(nBytes,
        PROLLY_NODE_ENTRY_BYTES(pNode->level, nLastKey, nLastVal),
        pLastKey, nLastKey, pNode->level);
  }

  *pWouldSplit = chunkBoundary(nBytes + thisSize, thisSize,
                               pKey, nKey, pNode->level);
}

static int appendWouldSplitNode(const ProllyNode *pNode,
                                const u8 *pKey, int nKey,
                                int nVal){
  int thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
  int nBytes = nodeTotalBytes(pNode) + thisSize;
  return chunkBoundary(nBytes, thisSize, pKey, nKey, pNode->level);
}

static int nodeEndsAtChunkBoundary(const ProllyNode *pNode){
  const u8 *pKey, *pVal;
  int nKey, nVal;
  int nBytes;

  if( pNode->nItems==0 ) return 0;
  nBytes = nodeTotalBytes(pNode);
  prollyNodeKey(pNode, (int)pNode->nItems - 1, &pKey, &nKey);
  prollyNodeValue(pNode, (int)pNode->nItems - 1, &pVal, &nVal);
  return chunkBoundary(nBytes,
                       PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal),
                       pKey, nKey, pNode->level);
}

static int nodeHasSameChunkShape(const ProllyNode *pNode,
                                 int requireFinalBoundary){
  const u8 *pKey, *pVal;
  int nKey, nVal;
  int nBytes = 0;
  int i;

  if( pNode->nItems==0 ) return 0;
  for(i=0; i<(int)pNode->nItems; i++){
    int isBoundary;
    int thisSize;

    prollyNodeKey(pNode, i, &pKey, &nKey);
    prollyNodeValue(pNode, i, &pVal, &nVal);
    (void)pVal;
    thisSize = PROLLY_NODE_ENTRY_BYTES(pNode->level, nKey, nVal);
    nBytes += thisSize;
    isBoundary = chunkBoundary(nBytes, thisSize, pKey, nKey, pNode->level);

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

static int writeOwnedNode(ChunkStore *pStore, ProllyCache *pCache,
                          u8 *pData, int nData, ProllyHash *pHash){
  int rc;
  rc = chunkStorePut(pStore, pData, nData, pHash);
  if( rc==SQLITE_OK && pCache ){
    ProllyCacheEntry *pEntry;
    pEntry = prollyCachePutOwned(pCache, pHash, pData, nData, &rc);
    pData = 0;
    if( pEntry ) prollyCacheRelease(pCache, pEntry);
  }
  sqlite3_free(pData);
  return rc;
}

static int writeBuilderNode(ChunkStore *pStore, ProllyCache *pCache,
                            ProllyNodeBuilder *pBuilder, ProllyHash *pHash){
  u8 *pData = 0;
  int nData = 0;
  int rc = prollyNodeBuilderFinish(pBuilder, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  return writeOwnedNode(pStore, pCache, pData, nData, pHash);
}

static u8 *copyNodeData(const ProllyNode *pNode){
  u8 *pData;
  if( pNode->nDataPhys!=pNode->nData ) return 0;
  sqlite3BeginBenignMalloc();
  pData = sqlite3_malloc(pNode->nData);
  sqlite3EndBenignMalloc();
  if( pData ) memcpy(pData, pNode->pData, pNode->nData);
  return pData;
}

static int finishAndWriteBuilderNode(ChunkStore *pStore, ProllyCache *pCache,
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
  if( rc==SQLITE_OK && pCache ){
    ProllyCacheEntry *pEntry;
    pEntry = prollyCachePutOwned(pCache, pHash, pData, nData, &rc);
    pData = 0;
    if( pEntry ) prollyCacheRelease(pCache, pEntry);
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
    u8 *pData;
    int idx;
    int i;

    level--;
    pNode = &pCur->aLevel[level].pEntry->node;
    idx = pCur->aLevel[level].idx;
    pData = copyNodeData(pNode);
    if( pData && prollyNodeHasSubtreeCounts(pNode) ){
      const u8 *pVal;
      int nVal;
      u64 cnt = prollyNodeChildSubtreeCount(pNode, idx);
      u8 *pCount;
      prollyNodeValue(pNode, idx, &pVal, &nVal);
      if( nVal!=PROLLY_HASH_SIZE ){
        sqlite3_free(pData);
        return SQLITE_CORRUPT;
      }
      if( countDelta<0 && cnt==0 ){
        sqlite3_free(pData);
        return SQLITE_CORRUPT;
      }
      if( countDelta<0 ) cnt--;
      if( countDelta>0 ) cnt++;
      memcpy(pData + (pVal - pNode->pData), pChildHash->data,
             PROLLY_HASH_SIZE);
      pCount = pData + (pNode->pSubtreeCounts - pNode->pData) + idx*8;
      pCount[0] = (u8)cnt;
      pCount[1] = (u8)(cnt >> 8);
      pCount[2] = (u8)(cnt >> 16);
      pCount[3] = (u8)(cnt >> 24);
      pCount[4] = (u8)(cnt >> 32);
      pCount[5] = (u8)(cnt >> 40);
      pCount[6] = (u8)(cnt >> 48);
      pCount[7] = (u8)(cnt >> 56);
      rc = writeOwnedNode(pMut->pStore, pMut->pCache, pData,
                          pNode->nData, &parentHash);
      if( rc!=SQLITE_OK ) return rc;
      *pChildHash = parentHash;
      continue;
    }
    sqlite3_free(pData);
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
            u64 newCnt = cnt;
            if( countDelta<0 ){
              newCnt--;
            }else if( countDelta>0 ){
              newCnt++;
            }
            prollyNodeKey(pNode, i, &pKey, &nKey);
            rc = prollyNodeBuilderAddWithCount(
                &b, pKey, nKey, pChildHash->data, PROLLY_HASH_SIZE,
                newCnt);
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
    rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &parentHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ) return rc;
    *pChildHash = parentHash;
  }

  return SQLITE_OK;
}

static int tryInsertOrReplaceSingleNoRechunk(ProllyMutator *pMut){
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
  /* These no-rechunk builders copy pVal verbatim; a symbolic zero tail
  ** needs the chunker path, which knows how to keep it sparse. */
  if( pEdit->nZeroTail ){
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
  if( cur.eState!=PROLLY_CURSOR_VALID ){
    prollyCursorClose(&cur);
    return SQLITE_NOTFOUND;
  }

  level = cur.iLevel;
  if( res==0 ){
    ProllyNode *pLeaf = &cur.aLevel[level].pEntry->node;
    int idx = cur.aLevel[level].idx;
    const u8 *pOldVal;
    u8 *pData;
    int nOldVal;
    ProllyNodeBuilder b;
    int sameSize;
    int i;

    prollyNodeValue(pLeaf, idx, &pOldVal, &nOldVal);
    sameSize = (nOldVal==pEdit->nVal);
    pData = sameSize ? copyNodeData(pLeaf) : 0;
    if( pData ){
      if( pEdit->nVal ){
        memcpy(pData + (pOldVal - pLeaf->pData), pEdit->pVal,
               pEdit->nVal);
      }
      rc = writeOwnedNode(pMut->pStore, pMut->pCache, pData,
                          pLeaf->nData, &childHash);
    }else{
      prollyNodeBuilderInit(&b, 0, pMut->flags);
      for(i=0; i<(int)pLeaf->nItems; i++){
        if( i==idx ){
          rc = prollyNodeBuilderAdd(&b, pEdit->pKey, pEdit->nKey,
                                    pEdit->pVal, pEdit->nVal);
        }else{
          rc = appendNodeEntryToBuilder(&b, pLeaf, i, 0);
        }
        if( rc!=SQLITE_OK ) break;
      }
      if( rc==SQLITE_OK ){
        if( sameSize ){
          rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b,
                                &childHash);
        }else{
          rc = finishAndWriteBuilderNode(pMut->pStore, pMut->pCache, &b,
                                         !cursorLeafIsRightmost(&cur),
                                         &childHash);
        }
      }
      prollyNodeBuilderFree(&b);
    }
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }

    rc = rewriteAncestorSpine(pMut, &cur, level, &childHash, 0);
    if( rc==SQLITE_OK ){
      pMut->newRoot = childHash;
    }
    prollyCursorClose(&cur);
    return rc;
  }else{
    ProllyNode *pLeaf;
    ProllyNodeBuilder b;
    int idx;
    int requireBoundary;
    int i;

    if( pMut->flags & PROLLY_NODE_INTKEY ){
      if( iKey>=prollyCursorIntKey(&cur) ){
        prollyCursorClose(&cur);
        return SQLITE_NOTFOUND;
      }
    }else{
      const u8 *pCurKey;
      int nCurKey;
      prollyCursorKey(&cur, &pCurKey, &nCurKey);
      if( prollyKeyCmp(pEdit->pKey, pEdit->nKey, pCurKey, nCurKey)>=0 ){
        prollyCursorClose(&cur);
        return SQLITE_NOTFOUND;
      }
    }

    pLeaf = &cur.aLevel[level].pEntry->node;
    idx = cur.aLevel[level].idx;
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
    rc = finishAndWriteBuilderNode(pMut->pStore, pMut->pCache, &b,
                                   requireBoundary, &childHash);
    prollyNodeBuilderFree(&b);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&cur);
      return rc;
    }

    rc = rewriteAncestorSpine(pMut, &cur, level, &childHash, 1);
    if( rc==SQLITE_OK ){
      pMut->newRoot = childHash;
    }
    prollyCursorClose(&cur);
    return rc;
  }
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
    rc = finishAndWriteBuilderNode(pMut->pStore, pMut->pCache, &b,
                                   requireBoundary, &childHash);
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

static int tryAppendSingleNoSplit(ProllyMutator *pMut){
  ProllyMutMapEntry *pEdit;
  ProllyCursor cur;
  ProllyHash childHash;
  const u8 *pNewKey;
  int nNewKey;
  i64 iNewKey;
  int rc;
  int res = 0;
  int level;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=1 ) return SQLITE_NOTFOUND;

  pEdit = &pMut->pEdits->aEntries[0];
  if( pEdit->op!=PROLLY_EDIT_INSERT ){
    return SQLITE_NOTFOUND;
  }
  if( pEdit->nZeroTail ){
    return SQLITE_NOTFOUND;
  }
  if( (pMut->flags & PROLLY_NODE_INTKEY) && pEdit->nKey!=8 ){
    return SQLITE_NOTFOUND;
  }

  pNewKey = pEdit->pKey;
  nNewKey = pEdit->nKey;
  if( pMut->flags & PROLLY_NODE_INTKEY ){
    iNewKey = prollyMutMapEntryIntKey(pEdit);
  }

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
  if( pMut->flags & PROLLY_NODE_INTKEY ){
    if( iNewKey <= prollyCursorIntKey(&cur) ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }
  }else{
    const u8 *pLastKey;
    int nLastKey;
    prollyCursorKey(&cur, &pLastKey, &nLastKey);
    if( prollyKeyCmp(pNewKey, nNewKey, pLastKey, nLastKey)<=0 ){
      prollyCursorClose(&cur);
      return SQLITE_NOTFOUND;
    }
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
          rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &childHash);
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
        rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &childHash);
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
          rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &parentHash);
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
          rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &childHash);
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
    rc = writeBuilderNode(pMut->pStore, pMut->pCache, &b, &parentHash);
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

static int tryDeleteInsertPairNoRechunk(ProllyMutator *pMut){
  ProllyMutator one;
  ProllyMutMap oneMap;
  ProllyMutMapEntry *pDel = 0;
  ProllyMutMapEntry *pIns = 0;
  ProllyHash midRoot;
  int i;
  int rc;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)!=2 ) return SQLITE_NOTFOUND;

  for(i=0; i<2; i++){
    ProllyMutMapEntry *pEntry = &pMut->pEdits->aEntries[i];
    if( pEntry->op==PROLLY_EDIT_DELETE ){
      if( pDel ) return SQLITE_NOTFOUND;
      pDel = pEntry;
    }else if( pEntry->op==PROLLY_EDIT_INSERT ){
      if( pIns ) return SQLITE_NOTFOUND;
      pIns = pEntry;
    }else{
      return SQLITE_NOTFOUND;
    }
  }
  if( !pDel || !pIns ) return SQLITE_NOTFOUND;

  memset(&oneMap, 0, sizeof(oneMap));
  oneMap.isIntKey = pMut->pEdits->isIntKey;
  oneMap.keepSorted = pMut->pEdits->keepSorted;
  oneMap.aEntries = pDel;
  oneMap.nEntries = 1;
  oneMap.nAlloc = 1;

  one = *pMut;
  one.pEdits = &oneMap;
  rc = tryDeleteSingleNoRechunk(&one);
  if( rc!=SQLITE_OK ) return rc;
  midRoot = one.newRoot;

  memset(&oneMap, 0, sizeof(oneMap));
  oneMap.isIntKey = pMut->pEdits->isIntKey;
  oneMap.keepSorted = pMut->pEdits->keepSorted;
  oneMap.aEntries = pIns;
  oneMap.nEntries = 1;
  oneMap.nAlloc = 1;

  one = *pMut;
  one.oldRoot = midRoot;
  one.pEdits = &oneMap;
  rc = tryInsertOrReplaceSingleNoRechunk(&one);
  if( rc!=SQLITE_OK ) return rc;

  pMut->newRoot = one.newRoot;
  return SQLITE_OK;
}

static int mutmapHasOnlyInserts(ProllyMutMap *pMap){
  int i;
  if( prollyMutMapIsEmpty(pMap) ) return 0;
  for(i=0; i<prollyMutMapCount(pMap); i++){
    if( pMap->aEntries[i].op!=PROLLY_EDIT_INSERT ){
      return 0;
    }
  }
  return 1;
}

static int batchReplacementProbe(ProllyMutator *pMut){
  ProllyCursor cur;
  int nEdits = prollyMutMapCount(pMut->pEdits);
  int nProbe = nEdits < 16 ? nEdits : 16;
  int i;
  int rc = SQLITE_OK;

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache, &pMut->oldRoot,
                   pMut->flags);
  for(i=0; i<nProbe; i++){
    ProllyMutMapEntry *pEdit;
    int idx = (int)(((i64)i * nEdits) / nProbe);
    int res = 0;
    rc = prollyMutMapEntryAt(pMut->pEdits, idx, &pEdit);
    if( rc!=SQLITE_OK ) break;
    if( pMut->flags & PROLLY_NODE_INTKEY ){
      rc = prollyCursorSeekInt(&cur, prollyMutMapEntryIntKey(pEdit), &res);
    }else{
      rc = prollyCursorSeekBlob(&cur, pEdit->pKey, pEdit->nKey, &res);
    }
    if( rc!=SQLITE_OK || res!=0 || !prollyCursorIsValid(&cur) ){
      if( rc==SQLITE_OK ) rc = SQLITE_NOTFOUND;
      break;
    }
    {
      const u8 *pOldVal;
      int nOldVal;
      prollyCursorValue(&cur, &pOldVal, &nOldVal);
      (void)pOldVal;
      if( pEdit->nZeroTail || pEdit->nVal!=nOldVal ){
        rc = SQLITE_NOTFOUND;
        break;
      }
    }
  }
  prollyCursorClose(&cur);
  return rc;
}

static int tryReplaceBatchLeafDirect(
  ProllyMutator *pMut,
  const ProllyNode *pLeaf,
  ProllyMutMapIter *pIter,
  int isLast,
  ProllyHash *pHash,
  int *pChanged
){
  ProllyMutMapIter iter = *pIter;
  u8 *pData = copyNodeData(pLeaf);
  int changed = 0;
  int i;
  int rc;

  if( pData==0 ) return SQLITE_NOTFOUND;
  for(i=0; i<(int)pLeaf->nItems; i++){
    const u8 *pCurKey;
    const u8 *pVal;
    int nCurKey;
    int nVal;
    ProllyMutMapEntry *pEd = 0;
    int cmp = 1;

    prollyNodeKey(pLeaf, i, &pCurKey, &nCurKey);
    prollyNodeValue(pLeaf, i, &pVal, &nVal);
    if( prollyMutMapIterValid(&iter) ){
      pEd = prollyMutMapIterEntry(&iter);
      cmp = prollyKeyCmp(pEd->pKey, pEd->nKey, pCurKey, nCurKey);
    }
    if( cmp<0 || (cmp==0 && (pEd->nZeroTail || pEd->nVal!=nVal)) ){
      sqlite3_free(pData);
      return SQLITE_NOTFOUND;
    }
    if( cmp==0 ){
      if( nVal>0 ){
        memcpy(pData + (pVal - pLeaf->pData), pEd->pVal, nVal);
      }
      changed = 1;
      prollyMutMapIterNext(&iter);
    }
  }
  if( isLast && prollyMutMapIterValid(&iter) ){
    sqlite3_free(pData);
    return SQLITE_NOTFOUND;
  }
  if( changed ){
    rc = writeOwnedNode(pMut->pStore, 0, pData, pLeaf->nData, pHash);
    pData = 0;
    if( rc!=SQLITE_OK ) return rc;
  }
  sqlite3_free(pData);
  *pIter = iter;
  *pChanged = changed;
  return SQLITE_OK;
}

static int replaceBatchLeafNoRechunk(
  ProllyMutator *pMut,
  const ProllyNode *pLeaf,
  ProllyMutMapIter *pIter,
  int isLast,
  ProllyHash *pHash,
  int *pChanged
){
  ProllyNodeBuilder b;
  int rc = SQLITE_OK;
  int changed = 0;
  int sizeChanged = 0;
  int i;

  rc = tryReplaceBatchLeafDirect(pMut, pLeaf, pIter, isLast,
                                 pHash, pChanged);
  if( rc!=SQLITE_NOTFOUND ) return rc;

  prollyNodeBuilderInit(&b, 0, pMut->flags);
  for(i=0; i<(int)pLeaf->nItems; i++){
    const u8 *pCurKey;
    const u8 *pVal;
    int nCurKey;
    int nVal;
    ProllyMutMapEntry *pEd = 0;
    int cmp = 1;

    prollyNodeKey(pLeaf, i, &pCurKey, &nCurKey);
    prollyNodeValue(pLeaf, i, &pVal, &nVal);
    if( prollyMutMapIterValid(pIter) ){
      pEd = prollyMutMapIterEntry(pIter);
      cmp = prollyKeyCmp(pEd->pKey, pEd->nKey, pCurKey, nCurKey);
    }
    if( cmp<0 ){
      prollyNodeBuilderFree(&b);
      return SQLITE_NOTFOUND;
    }else if( cmp==0 ){
      if( pEd->nZeroTail ){
        prollyNodeBuilderFree(&b);
        return SQLITE_NOTFOUND;
      }
      rc = prollyNodeBuilderAdd(&b, pCurKey, nCurKey,
                                pEd->pVal, pEd->nVal);
      changed = 1;
      if( pEd->nVal!=nVal ) sizeChanged = 1;
      prollyMutMapIterNext(pIter);
    }else{
      rc = prollyNodeBuilderAdd(&b, pCurKey, nCurKey, pVal, nVal);
    }
    if( rc!=SQLITE_OK ){
      prollyNodeBuilderFree(&b);
      return rc;
    }
  }

  if( isLast && prollyMutMapIterValid(pIter) ){
    prollyNodeBuilderFree(&b);
    return SQLITE_NOTFOUND;
  }

  if( changed ){
    if( sizeChanged ){
      /* A replacement of a different length moves every following entry, so the
      ** boundaries this leaf was chunked at may no longer be the ones a fresh
      ** build would pick. Validate and let the caller rechunk if they moved,
      ** the way the single-edit path does -- writing regardless freezes a shape
      ** that history independence says must not depend on how we got here. */
      rc = finishAndWriteBuilderNode(pMut->pStore, 0, &b, !isLast, pHash);
    }else{
      rc = writeBuilderNode(pMut->pStore, 0, &b, pHash);
    }
  }
  prollyNodeBuilderFree(&b);
  if( rc!=SQLITE_OK ) return rc;
  *pChanged = changed;
  return SQLITE_OK;
}

static int validateReplaceBatchLeaf(
  const ProllyNode *pLeaf,
  ProllyMutMapIter *pIter,
  int isLast
){
  int i;
  for(i=0; i<(int)pLeaf->nItems; i++){
    const u8 *pCurKey;
    int nCurKey;
    ProllyMutMapEntry *pEd = 0;
    int cmp = 1;

    prollyNodeKey(pLeaf, i, &pCurKey, &nCurKey);
    if( prollyMutMapIterValid(pIter) ){
      pEd = prollyMutMapIterEntry(pIter);
      cmp = prollyKeyCmp(pEd->pKey, pEd->nKey, pCurKey, nCurKey);
    }
    if( cmp<0 ){
      return SQLITE_NOTFOUND;
    }else if( cmp==0 ){
      prollyMutMapIterNext(pIter);
    }
  }
  if( isLast && prollyMutMapIterValid(pIter) ){
    return SQLITE_NOTFOUND;
  }
  return SQLITE_OK;
}

static int validateReplaceBatchNode(
  ProllyMutator *pMut,
  const ProllyNode *pNode,
  ProllyMutMapIter *pIter,
  int isLast
){
  int rc = SQLITE_OK;
  int i;

  if( pNode->level==0 ){
    return validateReplaceBatchLeaf(pNode, pIter, isLast);
  }

  for(i=0; i<(int)pNode->nItems; i++){
    const u8 *pBoundKey;
    const u8 *pChildVal;
    int nBoundKey;
    int nChildVal;
    int childIsLast = isLast && (i==(int)pNode->nItems - 1);
    int forceDescend = childIsLast && prollyMutMapIterValid(pIter);

    prollyNodeKey(pNode, i, &pBoundKey, &nBoundKey);
    prollyNodeValue(pNode, i, &pChildVal, &nChildVal);
    if( nChildVal!=PROLLY_HASH_SIZE ) return SQLITE_CORRUPT;
    if( forceDescend || subtreeHasEdits(pIter, pBoundKey, nBoundKey) ){
      ProllyHash childHash;
      ProllyCacheEntry *pChildEntry;

      memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
      rc = prollyLoadNode(pMut->pStore, pMut->pCache, &childHash, &pChildEntry);
      if( rc!=SQLITE_OK ) return rc;
      rc = validateReplaceBatchNode(pMut, &pChildEntry->node, pIter,
                                    childIsLast);
      prollyCacheRelease(pMut->pCache, pChildEntry);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int replaceBatchNodeNoRechunk(
  ProllyMutator *pMut,
  const ProllyNode *pNode,
  ProllyMutMapIter *pIter,
  int isLast,
  ProllyHash *pHash,
  int *pChanged
){
  ProllyNodeBuilder b;
  u8 *pData;
  int rc = SQLITE_OK;
  int changed = 0;
  int i;

  if( pNode->level==0 ){
    return replaceBatchLeafNoRechunk(pMut, pNode, pIter, isLast,
                                     pHash, pChanged);
  }

  prollyNodeBuilderInit(&b, pNode->level, pMut->flags);
  pData = copyNodeData(pNode);
  if( pData && !prollyNodeHasSubtreeCounts(pNode) ){
    sqlite3_free(pData);
    pData = 0;
  }
  for(i=0; i<(int)pNode->nItems; i++){
    const u8 *pBoundKey;
    const u8 *pChildVal;
    int nBoundKey;
    int nChildVal;
    int childIsLast = isLast && (i==(int)pNode->nItems - 1);
    int forceDescend = childIsLast && prollyMutMapIterValid(pIter);
    ProllyHash childHash;
    ProllyHash newChildHash;
    u64 childCount = 0;
    int childChanged = 0;

    prollyNodeKey(pNode, i, &pBoundKey, &nBoundKey);
    prollyNodeValue(pNode, i, &pChildVal, &nChildVal);
    if( nChildVal!=PROLLY_HASH_SIZE ){
      sqlite3_free(pData);
      prollyNodeBuilderFree(&b);
      return SQLITE_CORRUPT;
    }
    memcpy(&childHash, pChildVal, PROLLY_HASH_SIZE);
    newChildHash = childHash;
    if( pData==0 ){
      rc = parentChildSubtreeCount(pMut->pStore, pMut->pCache,
                                   pNode, i, &childCount);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pData);
        prollyNodeBuilderFree(&b);
        return rc;
      }
    }

    if( forceDescend || subtreeHasEdits(pIter, pBoundKey, nBoundKey) ){
      ProllyCacheEntry *pChildEntry;

      rc = prollyLoadNode(pMut->pStore, pMut->pCache, &childHash, &pChildEntry);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pData);
        prollyNodeBuilderFree(&b);
        return rc;
      }
      rc = replaceBatchNodeNoRechunk(pMut, &pChildEntry->node, pIter,
                                     childIsLast, &newChildHash,
                                     &childChanged);
      prollyCacheRelease(pMut->pCache, pChildEntry);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pData);
        prollyNodeBuilderFree(&b);
        return rc;
      }
      if( childChanged ){
        changed = 1;
        if( pData ){
          memcpy(pData + (pChildVal - pNode->pData), newChildHash.data,
                 PROLLY_HASH_SIZE);
        }
      }
    }

    if( pData==0 ){
      rc = prollyNodeBuilderAddWithCount(
          &b, pBoundKey, nBoundKey,
          (childChanged ? newChildHash.data : childHash.data),
          PROLLY_HASH_SIZE, childCount);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pData);
        prollyNodeBuilderFree(&b);
        return rc;
      }
    }
  }

  if( changed ){
    if( pData ){
      rc = writeOwnedNode(pMut->pStore, 0, pData, pNode->nData, pHash);
      pData = 0;
    }else{
      rc = writeBuilderNode(pMut->pStore, 0, &b, pHash);
    }
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&b);
  if( rc!=SQLITE_OK ) return rc;
  *pChanged = changed;
  return SQLITE_OK;
}

static int tryReplaceBatchNoRechunk(ProllyMutator *pMut){
  ProllyMutMapIter iter;
  ProllyCacheEntry *pRootEntry = 0;
  u8 *pRootData = 0;
  int nRootData = 0;
  ProllyNode rootNode;
  ProllyNode *pRootNode = &rootNode;
  ProllyHash newRoot;
  int changed = 0;
  int rc;

  if( prollyHashIsEmpty(&pMut->oldRoot) ) return SQLITE_NOTFOUND;
  if( prollyMutMapCount(pMut->pEdits)<=1 ) return SQLITE_NOTFOUND;
  if( !mutmapHasOnlyInserts(pMut->pEdits) ) return SQLITE_NOTFOUND;

  rc = batchReplacementProbe(pMut);
  if( rc!=SQLITE_OK ) return rc;

  pRootEntry = pMut->pCache ? prollyCacheGet(pMut->pCache, &pMut->oldRoot) : 0;
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

  rc = prollyMutMapIterFirst(&iter, pMut->pEdits);
  if( rc!=SQLITE_OK ) goto replace_batch_cleanup;
  if( prollyMutMapIterValid(&iter) && pRootNode->nItems>0 ){
    ProllyMutMapEntry *pFirst = prollyMutMapIterEntry(&iter);
    const u8 *pMaxKey;
    int nMaxKey;
    prollyNodeKey(pRootNode, (int)pRootNode->nItems - 1,
                  &pMaxKey, &nMaxKey);
    if( prollyKeyCmp(pFirst->pKey, pFirst->nKey, pMaxKey, nMaxKey)>0 ){
      rc = SQLITE_NOTFOUND;
      goto replace_batch_cleanup;
    }
  }
  rc = validateReplaceBatchNode(pMut, pRootNode, &iter, 1);
  if( rc==SQLITE_OK && prollyMutMapIterValid(&iter) ){
    rc = SQLITE_NOTFOUND;
  }
  if( rc==SQLITE_OK ){
    rc = prollyMutMapIterFirst(&iter, pMut->pEdits);
  }
  if( rc==SQLITE_OK ){
    rc = replaceBatchNodeNoRechunk(pMut, pRootNode, &iter, 1,
                                   &newRoot, &changed);
  }
  if( rc==SQLITE_OK ){
    pMut->newRoot = changed ? newRoot : pMut->oldRoot;
  }

replace_batch_cleanup:
  if( pRootEntry ) prollyCacheRelease(pMut->pCache, pRootEntry);
  sqlite3_free(pRootData);
  return rc;
}

int prollyMutateFlush(ProllyMutator *pMut){
  int rc;

  assert( pMut!=0 && pMut->pEdits!=0 && pMut->pStore!=0 );
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /* Materialize the edit sort order before any strategy iterates it. The
  ** iterator helpers compute it lazily and discard an allocation failure; an
  ** OOM there would leave a stale order and a strategy (e.g. buildFromEdits)
  ** would emit keys out of order, corrupting the tree. Fail cleanly instead. */
  rc = prollyMutMapEnsureOrder(pMut->pEdits);
  if( rc!=SQLITE_OK ) return rc;

  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    rc = buildFromEdits(pMut);
  }else{
    rc = tryInsertOrReplaceSingleNoRechunk(pMut);
    if( rc==SQLITE_NOTFOUND ){
      rc = tryAppendSingleNoSplit(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = tryDeleteSingleNoRechunk(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = tryDeleteInsertPairNoRechunk(pMut);
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = tryReplaceBatchNoRechunk(pMut);
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
      (void)prollyMutMapIterFirst(&it, pMut->pEdits);
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
      if( crc!=SQLITE_OK ){
        rc = crc;
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
