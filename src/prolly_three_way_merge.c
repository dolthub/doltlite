
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_merge.h"
#include "prolly_node.h"
#include "prolly_chunker.h"
#include "prolly_cursor.h"

#include <string.h>

/* Private sentinel: fast merge could not prove the result, so caller should
** fall back to the full row-wise merge path without treating it as an error. */
#define FM_FALLBACK  SQLITE_DONE

static int fmKeyCmp(const u8 *pA, int nA, const u8 *pB, int nB){
  int n = nA < nB ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  if( nA < nB ) return -1;
  if( nA > nB ) return 1;
  return 0;
}

static int fmChunkerLevelsBelowEmpty(const ProllyChunker *pCh, int level){
  int i;
  for( i = 0; i < level && i < pCh->nLevels; i++ ){
    if( pCh->aLevel[i].builder.nItems > 0 ) return 0;
  }
  return 1;
}

typedef struct FmCtx {
  ChunkStore *pStore;
  ProllyCache *pCache;
  u8 flags;
} FmCtx;

static int fmEmitChild(
  FmCtx *fm, ProllyChunker *pCh,
  const u8 *pBoundKey, int nBoundKey,
  int parentLevel,
  const ProllyHash *pAnc,
  const ProllyHash *pOurs,
  const ProllyHash *pTheirs
);

static int fmEmitSubtreeRows(
  FmCtx *fm, ProllyChunker *pCh,
  const ProllyHash *pSubtreeRoot
){
  u8 *pData = 0;
  int nData = 0;
  ProllyNode node;
  int rc;
  int i;

  rc = chunkStoreGet(fm->pStore, pSubtreeRoot, &pData, &nData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&node, pData, nData);
  if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }

  if( node.level == 0 ){
    for( i = 0; i < (int)node.nItems; i++ ){
      const u8 *pKey, *pVal;
      int nKey, nVal;
      prollyNodeKey(&node, i, &pKey, &nKey);
      prollyNodeValue(&node, i, &pVal, &nVal);
      rc = prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
      if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }
    }
  }else{
    for( i = 0; i < (int)node.nItems; i++ ){
      ProllyHash childHash;
      prollyNodeChildHash(&node, i, &childHash);
      rc = fmEmitSubtreeRows(fm, pCh, &childHash);
      if( rc != SQLITE_OK ){ sqlite3_free(pData); return rc; }
    }
  }
  sqlite3_free(pData);
  return SQLITE_OK;
}

static int fmResolveAndEmit(
  ProllyChunker *pCh,
  const u8 *pK, int nK,
  int has_a, const u8 *pAV, int nAV,
  int has_o, const u8 *pOV, int nOV,
  int has_t, const u8 *pTV, int nTV
){
  if( has_a && has_o && has_t ){
    int same_o = (nOV == nAV) && memcmp(pOV, pAV, nOV) == 0;
    int same_t = (nTV == nAV) && memcmp(pTV, pAV, nTV) == 0;
    if( same_o && same_t ){
      return prollyChunkerAdd(pCh, pK, nK, pAV, nAV);
    }else if( same_o ){
      return prollyChunkerAdd(pCh, pK, nK, pTV, nTV);
    }else if( same_t ){
      return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }else if( nOV == nTV && memcmp(pOV, pTV, nOV) == 0 ){
      return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }else{
      return FM_FALLBACK;
    }
  }else if( has_a && has_o && !has_t ){
    int same_o = (nOV == nAV) && memcmp(pOV, pAV, nOV) == 0;
    if( same_o ) return SQLITE_OK;
    return FM_FALLBACK;
  }else if( has_a && !has_o && has_t ){
    int same_t = (nTV == nAV) && memcmp(pTV, pAV, nTV) == 0;
    if( same_t ) return SQLITE_OK;
    return FM_FALLBACK;
  }else if( has_a && !has_o && !has_t ){
    return SQLITE_OK;
  }else if( !has_a && has_o && has_t ){
    if( nOV == nTV && memcmp(pOV, pTV, nOV) == 0 ){
      return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }
    return FM_FALLBACK;
  }else if( !has_a && has_o && !has_t ){
    return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
  }else if( !has_a && !has_o && has_t ){
    return prollyChunkerAdd(pCh, pK, nK, pTV, nTV);
  }
  return SQLITE_OK;
}

static int fmMergeLeaves(
  FmCtx *fm, ProllyChunker *pCh,
  const ProllyNode *pAncN,
  const ProllyNode *pOursN,
  const ProllyNode *pTheirsN
){
  int ai = 0, oi = 0, ti = 0;
  int rc = SQLITE_OK;

  while( ai < (int)pAncN->nItems
      || oi < (int)pOursN->nItems
      || ti < (int)pTheirsN->nItems ){
    const u8 *pAK = 0, *pOK = 0, *pTK = 0;
    int nAK = 0, nOK = 0, nTK = 0;
    const u8 *pAV = 0, *pOV = 0, *pTV = 0;
    int nAV = 0, nOV = 0, nTV = 0;
    i64 iAKey = 0, iOKey = 0, iTKey = 0;
    int has_a = 0, has_o = 0, has_t = 0;
    const u8 *pK = 0;
    int nK = 0;
    i64 iK = 0;

    if( ai < (int)pAncN->nItems ){
      prollyNodeKey(pAncN, ai, &pAK, &nAK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iAKey = prollyNodeIntKey(pAncN, ai);
    }
    if( oi < (int)pOursN->nItems ){
      prollyNodeKey(pOursN, oi, &pOK, &nOK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iOKey = prollyNodeIntKey(pOursN, oi);
    }
    if( ti < (int)pTheirsN->nItems ){
      prollyNodeKey(pTheirsN, ti, &pTK, &nTK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iTKey = prollyNodeIntKey(pTheirsN, ti);
    }

    {
      int hasAny[3];
      int minSide = -1;
      int s;
      const u8 *pCK; int nCK; i64 iCK;
      const u8 *pMK; int nMK; i64 iMK;
      int cmp;

      hasAny[0] = (ai < (int)pAncN->nItems);
      hasAny[1] = (oi < (int)pOursN->nItems);
      hasAny[2] = (ti < (int)pTheirsN->nItems);

      for( s = 0; s < 3; s++ ){
        if( !hasAny[s] ) continue;
        if( minSide < 0 ){ minSide = s; continue; }
        if( s == 0 ){ pCK = pAK; nCK = nAK; iCK = iAKey; }
        else if( s == 1 ){ pCK = pOK; nCK = nOK; iCK = iOKey; }
        else { pCK = pTK; nCK = nTK; iCK = iTKey; }
        if( minSide == 0 ){ pMK = pAK; nMK = nAK; iMK = iAKey; }
        else if( minSide == 1 ){ pMK = pOK; nMK = nOK; iMK = iOKey; }
        else { pMK = pTK; nMK = nTK; iMK = iTKey; }
        cmp = prollyCompareKeys(fm->flags,
                                 pCK, nCK, iCK,
                                 pMK, nMK, iMK);
        if( cmp < 0 ) minSide = s;
      }
      if( minSide == 0 ){ pK = pAK; nK = nAK; iK = iAKey; }
      else if( minSide == 1 ){ pK = pOK; nK = nOK; iK = iOKey; }
      else { pK = pTK; nK = nTK; iK = iTKey; }

      if( hasAny[0] ){
        cmp = prollyCompareKeys(fm->flags,
                                 pAK, nAK, iAKey,
                                 pK, nK, iK);
        has_a = (cmp == 0);
      }
      if( hasAny[1] ){
        cmp = prollyCompareKeys(fm->flags,
                                     pOK, nOK, iOKey,
                                     pK, nK, iK);
        has_o = (cmp == 0);
      }
      if( hasAny[2] ){
        cmp = prollyCompareKeys(fm->flags,
                                 pTK, nTK, iTKey,
                                 pK, nK, iK);
        has_t = (cmp == 0);
      }
    }

    if( has_a ) prollyNodeValue(pAncN, ai, &pAV, &nAV);
    if( has_o ) prollyNodeValue(pOursN, oi, &pOV, &nOV);
    if( has_t ) prollyNodeValue(pTheirsN, ti, &pTV, &nTV);

    rc = fmResolveAndEmit(pCh, pK, nK,
                           has_a, pAV, nAV,
                           has_o, pOV, nOV,
                           has_t, pTV, nTV);
    if( rc != SQLITE_OK ) return rc;

    if( has_a ) ai++;
    if( has_o ) oi++;
    if( has_t ) ti++;
  }

  return SQLITE_OK;
}

static int fmCursorMerge(
  FmCtx *fm, ProllyChunker *pCh,
  ProllyCursor *pAncC, ProllyCursor *pOursC, ProllyCursor *pTheirsC
){
  int rc;

  while( prollyCursorIsValid(pAncC)
      || prollyCursorIsValid(pOursC)
      || prollyCursorIsValid(pTheirsC) ){
    int hasAny[3];
    const u8 *pAK = 0, *pOK = 0, *pTK = 0;
    int nAK = 0, nOK = 0, nTK = 0;
    i64 iAKey = 0, iOKey = 0, iTKey = 0;
    const u8 *pAV = 0, *pOV = 0, *pTV = 0;
    int nAV = 0, nOV = 0, nTV = 0;
    int has_a = 0, has_o = 0, has_t = 0;
    const u8 *pK; int nK; i64 iK;
    int minSide = -1;
    int s, cmp;
    const u8 *pCK; int nCK; i64 iCK;
    const u8 *pMK; int nMK; i64 iMK;

    hasAny[0] = prollyCursorIsValid(pAncC);
    hasAny[1] = prollyCursorIsValid(pOursC);
    hasAny[2] = prollyCursorIsValid(pTheirsC);

    if( hasAny[0] ){
      prollyCursorKey(pAncC, &pAK, &nAK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iAKey = prollyCursorIntKey(pAncC);
    }
    if( hasAny[1] ){
      prollyCursorKey(pOursC, &pOK, &nOK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iOKey = prollyCursorIntKey(pOursC);
    }
    if( hasAny[2] ){
      prollyCursorKey(pTheirsC, &pTK, &nTK);
      if( fm->flags & PROLLY_NODE_INTKEY ) iTKey = prollyCursorIntKey(pTheirsC);
    }

    for( s = 0; s < 3; s++ ){
      if( !hasAny[s] ) continue;
      if( minSide < 0 ){ minSide = s; continue; }
      if( s == 0 ){ pCK = pAK; nCK = nAK; iCK = iAKey; }
      else if( s == 1 ){ pCK = pOK; nCK = nOK; iCK = iOKey; }
      else { pCK = pTK; nCK = nTK; iCK = iTKey; }
      if( minSide == 0 ){ pMK = pAK; nMK = nAK; iMK = iAKey; }
      else if( minSide == 1 ){ pMK = pOK; nMK = nOK; iMK = iOKey; }
      else { pMK = pTK; nMK = nTK; iMK = iTKey; }
      cmp = prollyCompareKeys(fm->flags, pCK, nCK, iCK, pMK, nMK, iMK);
      if( cmp < 0 ) minSide = s;
    }
    if( minSide == 0 ){ pK = pAK; nK = nAK; iK = iAKey; }
    else if( minSide == 1 ){ pK = pOK; nK = nOK; iK = iOKey; }
    else { pK = pTK; nK = nTK; iK = iTKey; }

    if( hasAny[0] ){
      cmp = prollyCompareKeys(fm->flags, pAK, nAK, iAKey, pK, nK, iK);
      has_a = (cmp == 0);
    }
    if( hasAny[1] ){
      cmp = prollyCompareKeys(fm->flags, pOK, nOK, iOKey, pK, nK, iK);
      has_o = (cmp == 0);
    }
    if( hasAny[2] ){
      cmp = prollyCompareKeys(fm->flags, pTK, nTK, iTKey, pK, nK, iK);
      has_t = (cmp == 0);
    }

    if( has_a ) prollyCursorValue(pAncC, &pAV, &nAV);
    if( has_o ) prollyCursorValue(pOursC, &pOV, &nOV);
    if( has_t ) prollyCursorValue(pTheirsC, &pTV, &nTV);

    rc = fmResolveAndEmit(pCh, pK, nK,
                           has_a, pAV, nAV,
                           has_o, pOV, nOV,
                           has_t, pTV, nTV);
    if( rc != SQLITE_OK ) return rc;

    if( has_a ){ rc = prollyCursorNext(pAncC); if( rc != SQLITE_OK ) return rc; }
    if( has_o ){ rc = prollyCursorNext(pOursC); if( rc != SQLITE_OK ) return rc; }
    if( has_t ){ rc = prollyCursorNext(pTheirsC); if( rc != SQLITE_OK ) return rc; }
  }

  return SQLITE_OK;
}

static int fmCursorRecover(
  FmCtx *fm, ProllyChunker *pCh,
  const ProllyHash *pAncH,
  const ProllyHash *pOursH,
  const ProllyHash *pTheirsH,
  const u8 *pSeekKey, int nSeekKey, i64 iSeekKey
){
  ProllyCursor curA, curO, curT;
  int initA = 0, initO = 0, initT = 0;
  int rc;
  int res;

  prollyCursorInit(&curA, fm->pStore, fm->pCache, pAncH, fm->flags); initA = 1;
  prollyCursorInit(&curO, fm->pStore, fm->pCache, pOursH, fm->flags); initO = 1;
  prollyCursorInit(&curT, fm->pStore, fm->pCache, pTheirsH, fm->flags); initT = 1;

  if( pSeekKey == 0 ){
    rc = prollyCursorFirst(&curA, &res);
    if( rc == SQLITE_OK ) rc = prollyCursorFirst(&curO, &res);
    if( rc == SQLITE_OK ) rc = prollyCursorFirst(&curT, &res);
  }else{
    if( fm->flags & PROLLY_NODE_INTKEY ){
      rc = prollyCursorSeekInt(&curA, iSeekKey, &res);
      if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curA);
      if( rc == SQLITE_OK ){
        rc = prollyCursorSeekInt(&curO, iSeekKey, &res);
        if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curO);
      }
      if( rc == SQLITE_OK ){
        rc = prollyCursorSeekInt(&curT, iSeekKey, &res);
        if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curT);
      }
    }else{
      rc = prollyCursorSeekBlob(&curA, pSeekKey, nSeekKey, &res);
      if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curA);
      if( rc == SQLITE_OK ){
        rc = prollyCursorSeekBlob(&curO, pSeekKey, nSeekKey, &res);
        if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curO);
      }
      if( rc == SQLITE_OK ){
        rc = prollyCursorSeekBlob(&curT, pSeekKey, nSeekKey, &res);
        if( rc == SQLITE_OK && res == 0 ) rc = prollyCursorNext(&curT);
      }
    }
  }

  if( rc == SQLITE_OK ){
    rc = fmCursorMerge(fm, pCh, &curA, &curO, &curT);
  }

  if( initA ) prollyCursorClose(&curA);
  if( initO ) prollyCursorClose(&curO);
  if( initT ) prollyCursorClose(&curT);
  return rc;
}

static int fmWalkInterior(
  FmCtx *fm, ProllyChunker *pCh,
  ProllyNode *pAncN, ProllyNode *pOursN, ProllyNode *pTheirsN,
  const ProllyHash *pAncH, const ProllyHash *pOursH, const ProllyHash *pTheirsH
){
  int i;
  int parentLevel = pAncN->level;

  if( pOursN->nItems != pAncN->nItems
   || pTheirsN->nItems != pAncN->nItems ){
    return fmCursorRecover(fm, pCh, pAncH, pOursH, pTheirsH, 0, 0, 0);
  }

  for( i = 0; i < (int)pAncN->nItems; i++ ){
    const u8 *pAK; int nAK;
    const u8 *pOK; int nOK;
    const u8 *pTK; int nTK;
    ProllyHash hAnc, hOurs, hTheirs;
    int rc;

    prollyNodeKey(pAncN, i, &pAK, &nAK);
    prollyNodeKey(pOursN, i, &pOK, &nOK);
    prollyNodeKey(pTheirsN, i, &pTK, &nTK);

    if( fmKeyCmp(pAK, nAK, pOK, nOK) != 0
     || fmKeyCmp(pAK, nAK, pTK, nTK) != 0 ){
      const u8 *pSeek = 0;
      int nSeek = 0;
      i64 iSeek = 0;
      if( i > 0 ){
        prollyNodeKey(pAncN, i-1, &pSeek, &nSeek);
        if( fm->flags & PROLLY_NODE_INTKEY ){
          iSeek = prollyNodeIntKey(pAncN, i-1);
        }
      }
      return fmCursorRecover(fm, pCh, pAncH, pOursH, pTheirsH,
                              pSeek, nSeek, iSeek);
    }

    prollyNodeChildHash(pAncN, i, &hAnc);
    prollyNodeChildHash(pOursN, i, &hOurs);
    prollyNodeChildHash(pTheirsN, i, &hTheirs);

    rc = fmEmitChild(fm, pCh, pAK, nAK, parentLevel, &hAnc, &hOurs, &hTheirs);
    if( rc != SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

static int fmEmitChild(
  FmCtx *fm, ProllyChunker *pCh,
  const u8 *pBoundKey, int nBoundKey,
  int parentLevel,
  const ProllyHash *pAnc,
  const ProllyHash *pOurs,
  const ProllyHash *pTheirs
){
  const ProllyHash *pSplice = 0;
  u8 *pAncData = 0, *pOursData = 0, *pTheirsData = 0;
  int nAncData = 0, nOursData = 0, nTheirsData = 0;
  ProllyNode ancNode, oursNode, theirsNode;
  int rc;

  if( prollyHashCompare(pOurs, pTheirs) == 0 )      pSplice = pOurs;
  else if( prollyHashCompare(pOurs, pAnc) == 0 )    pSplice = pTheirs;
  else if( prollyHashCompare(pTheirs, pAnc) == 0 )  pSplice = pOurs;

  if( pSplice ){
    /* Preserve structural sharing only when the chunker is exactly aligned at
    ** this parent level; otherwise emit rows so the rebuilt tree stays sorted. */
    if( fmChunkerLevelsBelowEmpty(pCh, parentLevel) ){
      return prollyChunkerAddAtLevel(pCh, parentLevel,
                                      pBoundKey, nBoundKey,
                                      pSplice->data, PROLLY_HASH_SIZE);
    }
    return fmEmitSubtreeRows(fm, pCh, pSplice);
  }

  rc = chunkStoreGet(fm->pStore, pAnc, &pAncData, &nAncData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&ancNode, pAncData, nAncData);
  if( rc != SQLITE_OK ) goto done;

  rc = chunkStoreGet(fm->pStore, pOurs, &pOursData, &nOursData);
  if( rc != SQLITE_OK ) goto done;
  rc = prollyNodeParse(&oursNode, pOursData, nOursData);
  if( rc != SQLITE_OK ) goto done;

  rc = chunkStoreGet(fm->pStore, pTheirs, &pTheirsData, &nTheirsData);
  if( rc != SQLITE_OK ) goto done;
  rc = prollyNodeParse(&theirsNode, pTheirsData, nTheirsData);
  if( rc != SQLITE_OK ) goto done;

  if( oursNode.level != ancNode.level || theirsNode.level != ancNode.level ){
    rc = FM_FALLBACK;
    goto done;
  }

  if( ancNode.level == 0 ){
    rc = fmMergeLeaves(fm, pCh, &ancNode, &oursNode, &theirsNode);
  }else{
    rc = fmWalkInterior(fm, pCh, &ancNode, &oursNode, &theirsNode,
                         pAnc, pOurs, pTheirs);
  }

done:
  sqlite3_free(pAncData);
  sqlite3_free(pOursData);
  sqlite3_free(pTheirsData);
  return rc;
}

int prollyThreeWayMergeFast(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ProllyHash *pMergedRoot,
  int *pHandled
){
  FmCtx fm;
  ProllyChunker chunker;
  u8 *pAncData = 0, *pOursData = 0, *pTheirsData = 0;
  int nAncData = 0, nOursData = 0, nTheirsData = 0;
  ProllyNode ancNode, oursNode, theirsNode;
  int rc = SQLITE_OK;

  *pHandled = 0;

  if( prollyHashCompare(pOursRoot, pTheirsRoot) == 0 ){
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }
  if( prollyHashCompare(pOursRoot, pAncRoot) == 0 ){
    memcpy(pMergedRoot, pTheirsRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }
  if( prollyHashCompare(pTheirsRoot, pAncRoot) == 0 ){
    memcpy(pMergedRoot, pOursRoot, sizeof(ProllyHash));
    *pHandled = 1;
    return SQLITE_OK;
  }

  if( prollyHashIsEmpty(pAncRoot)
   || prollyHashIsEmpty(pOursRoot)
   || prollyHashIsEmpty(pTheirsRoot) ){
    return SQLITE_OK;
  }

  rc = chunkStoreGet(pStore, pAncRoot, &pAncData, &nAncData);
  if( rc != SQLITE_OK ) return rc;
  rc = prollyNodeParse(&ancNode, pAncData, nAncData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); return rc; }

  rc = chunkStoreGet(pStore, pOursRoot, &pOursData, &nOursData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); return rc; }
  rc = prollyNodeParse(&oursNode, pOursData, nOursData);
  if( rc != SQLITE_OK ){ sqlite3_free(pAncData); sqlite3_free(pOursData); return rc; }

  rc = chunkStoreGet(pStore, pTheirsRoot, &pTheirsData, &nTheirsData);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData);
    return rc;
  }
  rc = prollyNodeParse(&theirsNode, pTheirsData, nTheirsData);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return rc;
  }

  if( oursNode.level != ancNode.level || theirsNode.level != ancNode.level ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return SQLITE_OK;
  }

  fm.pStore = pStore;
  fm.pCache = pCache;
  fm.flags = flags;

  rc = prollyChunkerInit(&chunker, pStore, flags);
  if( rc != SQLITE_OK ){
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    return rc;
  }

  if( ancNode.level == 0 ){
    rc = fmMergeLeaves(&fm, &chunker, &ancNode, &oursNode, &theirsNode);
    sqlite3_free(pAncData); sqlite3_free(pOursData); sqlite3_free(pTheirsData);
    if( rc == SQLITE_OK ){
      rc = prollyChunkerFinish(&chunker);
      if( rc == SQLITE_OK ){
        prollyChunkerGetRoot(&chunker, pMergedRoot);
        *pHandled = 1;
      }
    }else if( rc == FM_FALLBACK ){
      rc = SQLITE_OK;
    }
    prollyChunkerFree(&chunker);
    return rc;
  }

  rc = fmWalkInterior(&fm, &chunker, &ancNode, &oursNode, &theirsNode,
                       pAncRoot, pOursRoot, pTheirsRoot);

  sqlite3_free(pAncData);
  sqlite3_free(pOursData);
  sqlite3_free(pTheirsData);

  if( rc == SQLITE_OK ){
    rc = prollyChunkerFinish(&chunker);
    if( rc == SQLITE_OK ){
      prollyChunkerGetRoot(&chunker, pMergedRoot);
      *pHandled = 1;
    }
  }else if( rc == FM_FALLBACK ){
    rc = SQLITE_OK;
  }
  prollyChunkerFree(&chunker);
  return rc;
}

#endif
