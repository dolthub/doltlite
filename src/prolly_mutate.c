
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

/* Encode signed i64 as big-endian sort key. XOR sign bit so memcmp gives
** correct signed ordering. BIG-ENDIAN despite the rest of the system being
** little-endian -- sort keys must compare correctly under byte-wise memcmp. */
static void encodeI64BE(u8 *buf, i64 v){
  u64 u = (u64)v ^ ((u64)1 << 63);
  buf[0] = (u8)(u >> 56);
  buf[1] = (u8)(u >> 48);
  buf[2] = (u8)(u >> 40);
  buf[3] = (u8)(u >> 32);
  buf[4] = (u8)(u >> 24);
  buf[5] = (u8)(u >> 16);
  buf[6] = (u8)(u >> 8);
  buf[7] = (u8)(u);
}

#define PROLLY_EST_ENTRIES_PER_LEAF 50

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

static int feedChunker(
  ProllyChunker *pCh,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  if( flags & PROLLY_NODE_INTKEY ){
    
    u8 aKeyBuf[8];
    encodeI64BE(aKeyBuf, intKey);
    return prollyChunkerAdd(pCh, aKeyBuf, 8, pVal, nVal);
  }else{
    return prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
  }
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
  return (cmp <= 0);  
}

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
      
      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
      continue;
    }

    
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
        
        const u8 *pVal; int nVal;
        prollyNodeValue(pLeaf, j, &pVal, &nVal);
        rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }
    }

    
    int cmp = compareKeys(flags, pCurKey, nCurKey, iCurKey,
                          pEd->pKey, pEd->nKey, pEd->intKey);
    if( cmp < 0 ){
      
      const u8 *pVal; int nVal;
      prollyNodeValue(pLeaf, j, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pCurKey, nCurKey, pVal, nVal);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else if( cmp == 0 ){
      
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

/* Streaming merge: skip unmodified subtrees by re-linking their hash at the
** parent level (prollyChunkerAddAtLevel). Only descend into children whose
** key range overlaps pending edits. O(M*L) when M << N. Falls through to
** mergeWalk for height-0 (single-leaf) trees. */
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
        /* No edits AND level-0 builder is empty (at chunk boundary).
        ** Re-link child hash at parent level to skip this subtree.
        ** Cannot skip if level-0 has pending items: that would break
        ** chunk boundary alignment with the new tree. */
        rc = prollyChunkerAddAtLevel(&chunker, rootNode.level,
                                      pBoundKey, nBoundKey,
                                      pChildVal, nChildVal);
        if( rc!=SQLITE_OK ) goto streaming_cleanup;
      }else{
        
        if( rootNode.level == 1 ){
          
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

/* Apply pending edits to produce a new tree root. Chooses between
** streamingMerge (skips unchanged subtrees, good when M << N) and
** mergeWalk (full scan, better when M ~ N) based on estimated tree size. */
int prollyMutateFlush(ProllyMutator *pMut){
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  
  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  
  {
    int M = prollyMutMapCount(pMut->pEdits);
    int N = 0;

    
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
          
          int factor = 1;
          int lv;
          for(lv = 0; lv < rootNode.level; lv++) factor *= PROLLY_EST_ENTRIES_PER_LEAF;
          N = rootNode.nItems * factor;
        }
      }
      sqlite3_free(pRootData);
    }

    
    int threshold;
    if( N <= 0 ){
      threshold = 1000;  
    }else{
      threshold = N / 2;
    }

    
    if( M > threshold ){
      return mergeWalk(pMut);
    }else{
      return streamingMerge(pMut);
    }
  }
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
