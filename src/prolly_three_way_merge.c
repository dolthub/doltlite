
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_merge.h"
#include "prolly_node.h"
#include "prolly_chunker.h"
#include "prolly_cursor.h"
#include "prolly_diff.h"

#include <string.h>

/* Private sentinel: fast merge could not prove the result, so caller should
** fall back to the full row-wise merge path without treating it as an error. */
#define FM_FALLBACK  SQLITE_DONE

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

static int fmEq(const u8 *pX, int nX, const u8 *pY, int nY){
  int equal = 0;
  if( prollyValuesEqual(pX, nX, pY, nY, &equal)!=SQLITE_OK ) return 0;
  return equal;
}

static int fmResolveAndEmit(
  ProllyChunker *pCh,
  const u8 *pK, int nK,
  int has_a, const u8 *pAV, int nAV,
  int has_o, const u8 *pOV, int nOV,
  int has_t, const u8 *pTV, int nTV
){
  if( has_a && has_o && has_t ){
    int same_o = fmEq(pOV, nOV, pAV, nAV);
    int same_t = fmEq(pTV, nTV, pAV, nAV);
    if( same_o && same_t ){
      return prollyChunkerAdd(pCh, pK, nK, pAV, nAV);
    }else if( same_o ){
      return prollyChunkerAdd(pCh, pK, nK, pTV, nTV);
    }else if( same_t ){
      return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }else if( fmEq(pOV, nOV, pTV, nTV) ){
      return prollyChunkerAdd(pCh, pK, nK, pOV, nOV);
    }else{
      return FM_FALLBACK;
    }
  }else if( has_a && has_o && !has_t ){
    if( fmEq(pOV, nOV, pAV, nAV) ) return SQLITE_OK;
    return FM_FALLBACK;
  }else if( has_a && !has_o && has_t ){
    if( fmEq(pTV, nTV, pAV, nAV) ) return SQLITE_OK;
    return FM_FALLBACK;
  }else if( has_a && !has_o && !has_t ){
    return SQLITE_OK;
  }else if( !has_a && has_o && has_t ){
    if( fmEq(pOV, nOV, pTV, nTV) ){
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

typedef struct FmKey FmKey;
struct FmKey {
  const u8 *p;
  int n;
  i64 i;
  int has;
};

static void fmKeyFromNode(u8 flags, const ProllyNode *pN, int idx, FmKey *pK){
  pK->p = 0; pK->n = 0; pK->i = 0;
  pK->has = idx < (int)pN->nItems;
  if( pK->has ){
    prollyNodeKey(pN, idx, &pK->p, &pK->n);
    if( flags & PROLLY_NODE_INTKEY ) pK->i = prollyNodeIntKey(pN, idx);
  }
}

static void fmKeyFromCursor(u8 flags, ProllyCursor *pC, FmKey *pK){
  pK->p = 0; pK->n = 0; pK->i = 0;
  pK->has = prollyCursorIsValid(pC);
  if( pK->has ){
    prollyCursorKey(pC, &pK->p, &pK->n);
    if( flags & PROLLY_NODE_INTKEY ) pK->i = prollyCursorIntKey(pC);
  }
}

/* Pick the smallest of the three present keys into *pMin, then reset each
** input's .has to whether that side actually holds the min key (the rest of
** the merge advances exactly the sides flagged here). */
static void fmSelectMinKey(u8 flags, FmKey *pA, FmKey *pO, FmKey *pT, FmKey *pMin){
  FmKey *aSide[3];
  int minSide = -1;
  int s;
  aSide[0] = pA; aSide[1] = pO; aSide[2] = pT;
  for( s = 0; s < 3; s++ ){
    if( !aSide[s]->has ) continue;
    if( minSide < 0
     || prollyCompareKeys(flags, aSide[s]->p, aSide[s]->n, aSide[s]->i,
                          aSide[minSide]->p, aSide[minSide]->n, aSide[minSide]->i) < 0 ){
      minSide = s;
    }
  }
  *pMin = *aSide[minSide];
  for( s = 0; s < 3; s++ ){
    if( aSide[s]->has ){
      aSide[s]->has = prollyCompareKeys(flags, aSide[s]->p, aSide[s]->n, aSide[s]->i,
                                        pMin->p, pMin->n, pMin->i) == 0;
    }
  }
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
    FmKey ka, ko, kt, kmin;
    const u8 *pAV = 0, *pOV = 0, *pTV = 0;
    int nAV = 0, nOV = 0, nTV = 0;

    fmKeyFromNode(fm->flags, pAncN, ai, &ka);
    fmKeyFromNode(fm->flags, pOursN, oi, &ko);
    fmKeyFromNode(fm->flags, pTheirsN, ti, &kt);
    fmSelectMinKey(fm->flags, &ka, &ko, &kt, &kmin);

    if( ka.has ) prollyNodeValue(pAncN, ai, &pAV, &nAV);
    if( ko.has ) prollyNodeValue(pOursN, oi, &pOV, &nOV);
    if( kt.has ) prollyNodeValue(pTheirsN, ti, &pTV, &nTV);

    rc = fmResolveAndEmit(pCh, kmin.p, kmin.n,
                           ka.has, pAV, nAV,
                           ko.has, pOV, nOV,
                           kt.has, pTV, nTV);
    if( rc != SQLITE_OK ) return rc;

    if( ka.has ) ai++;
    if( ko.has ) oi++;
    if( kt.has ) ti++;
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
    FmKey ka, ko, kt, kmin;
    const u8 *pAV = 0, *pOV = 0, *pTV = 0;
    int nAV = 0, nOV = 0, nTV = 0;

    fmKeyFromCursor(fm->flags, pAncC, &ka);
    fmKeyFromCursor(fm->flags, pOursC, &ko);
    fmKeyFromCursor(fm->flags, pTheirsC, &kt);
    fmSelectMinKey(fm->flags, &ka, &ko, &kt, &kmin);

    if( ka.has ) prollyCursorValue(pAncC, &pAV, &nAV);
    if( ko.has ) prollyCursorValue(pOursC, &pOV, &nOV);
    if( kt.has ) prollyCursorValue(pTheirsC, &pTV, &nTV);

    rc = fmResolveAndEmit(pCh, kmin.p, kmin.n,
                           ka.has, pAV, nAV,
                           ko.has, pOV, nOV,
                           kt.has, pTV, nTV);
    if( rc != SQLITE_OK ) return rc;

    if( ka.has ){ rc = prollyCursorNext(pAncC); if( rc != SQLITE_OK ) return rc; }
    if( ko.has ){ rc = prollyCursorNext(pOursC); if( rc != SQLITE_OK ) return rc; }
    if( kt.has ){ rc = prollyCursorNext(pTheirsC); if( rc != SQLITE_OK ) return rc; }
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

    if( prollyKeyCmp(pAK, nAK, pOK, nOK) != 0
     || prollyKeyCmp(pAK, nAK, pTK, nTK) != 0 ){
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
    if( prollyChunkerLevelsBelowEmpty(pCh, parentLevel) ){
      u64 spliceCount = 0;
      rc = prollySubtreeCount(fm->pStore, fm->pCache, pSplice, &spliceCount);
      if( rc != SQLITE_OK ) return rc;
      return prollyChunkerAddAtLevelWithCount(pCh, parentLevel,
                                              pBoundKey, nBoundKey,
                                              pSplice->data, PROLLY_HASH_SIZE,
                                              spliceCount);
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
