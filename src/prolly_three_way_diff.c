
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_diff.h"
#include <string.h>

/* Streaming three-way diff.
**
** Instead of collecting all changes from both sides into memory
** arrays (O(n) memory), this implementation runs two ProllyDiffIter
** cursors in parallel and merge-joins them on the fly.  Memory usage
** is O(1) — only the two "current change" structs from the iterators
** are alive at any point. */

static int valuesEqual(const u8 *pA, int nA, const u8 *pB, int nB){
  int equal = 0;
  return prollyValuesEqual(pA, nA, pB, nB, &equal)==SQLITE_OK && equal;
}

static int diffChangeKeyCmp(
  const ProllyDiffChange *pA,
  const ProllyDiffChange *pB,
  u8 flags
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( pA->intKey < pB->intKey ) return -1;
    if( pA->intKey > pB->intKey ) return 1;
    return 0;
  }else{
    int n = (pA->nKey < pB->nKey) ? pA->nKey : pB->nKey;
    int c = memcmp(pA->pKey, pB->pKey, n);
    if( c ) return c;
    return pA->nKey - pB->nKey;
  }
}

static void fillKeyFromChange(ThreeWayChange *pOut,
                              const ProllyDiffChange *pCh){
  pOut->pKey = pCh->pKey;
  pOut->nKey = pCh->nKey;
  pOut->intKey = pCh->intKey;
}

static int emitLeftOnly(
  const ProllyDiffChange *pLeft,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromChange(&change, pLeft);

  switch( pLeft->type ){
    case PROLLY_DIFF_ADD:
      change.type = THREE_WAY_LEFT_ADD;
      change.pOurVal = pLeft->pNewVal;
      change.nOurVal = pLeft->nNewVal;
      break;
    case PROLLY_DIFF_DELETE:
      change.type = THREE_WAY_LEFT_DELETE;
      change.pBaseVal = pLeft->pOldVal;
      change.nBaseVal = pLeft->nOldVal;
      break;
    case PROLLY_DIFF_MODIFY:
      change.type = THREE_WAY_LEFT_MODIFY;
      change.pBaseVal = pLeft->pOldVal;
      change.nBaseVal = pLeft->nOldVal;
      change.pOurVal = pLeft->pNewVal;
      change.nOurVal = pLeft->nNewVal;
      break;
  }
  return xCallback(pCtx, &change);
}

static int emitRightOnly(
  const ProllyDiffChange *pRight,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromChange(&change, pRight);

  switch( pRight->type ){
    case PROLLY_DIFF_ADD:
      change.type = THREE_WAY_RIGHT_ADD;
      change.pTheirVal = pRight->pNewVal;
      change.nTheirVal = pRight->nNewVal;
      break;
    case PROLLY_DIFF_DELETE:
      change.type = THREE_WAY_RIGHT_DELETE;
      change.pBaseVal = pRight->pOldVal;
      change.nBaseVal = pRight->nOldVal;
      break;
    case PROLLY_DIFF_MODIFY:
      change.type = THREE_WAY_RIGHT_MODIFY;
      change.pBaseVal = pRight->pOldVal;
      change.nBaseVal = pRight->nOldVal;
      change.pTheirVal = pRight->pNewVal;
      change.nTheirVal = pRight->nNewVal;
      break;
  }
  return xCallback(pCtx, &change);
}

/* Classification when both branches touched the same key:
**
**   ADD vs ADD        -> CONVERGENT if same value, else CONFLICT_MM
**   DELETE vs DELETE  -> CONVERGENT
**   MODIFY vs MODIFY  -> CONVERGENT if same new value, else CONFLICT_MM
**   DELETE vs MODIFY  -> CONFLICT_DM
**   MODIFY vs DELETE  -> CONFLICT_DM
**   anything else     -> CONFLICT_MM (fallback)
**
** "Both sides made the same change" (convergent) is the one case
** merge can silently accept -- everything else either needs user
** resolution or is impossible given the diff semantics. */
static int emitBothSides(
  const ProllyDiffChange *pLeft,
  const ProllyDiffChange *pRight,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromChange(&change, pLeft);


  if( pLeft->type==PROLLY_DIFF_ADD && pRight->type==PROLLY_DIFF_ADD ){
    if( valuesEqual(pLeft->pNewVal, pLeft->nNewVal,
                    pRight->pNewVal, pRight->nNewVal) ){
      change.type = THREE_WAY_CONVERGENT;
    }else{
      change.type = THREE_WAY_CONFLICT_MM;
    }
    change.pOurVal = pLeft->pNewVal;
    change.nOurVal = pLeft->nNewVal;
    change.pTheirVal = pRight->pNewVal;
    change.nTheirVal = pRight->nNewVal;
    return xCallback(pCtx, &change);
  }


  if( pLeft->type==PROLLY_DIFF_DELETE && pRight->type==PROLLY_DIFF_DELETE ){
    change.type = THREE_WAY_CONVERGENT;
    change.pBaseVal = pLeft->pOldVal;
    change.nBaseVal = pLeft->nOldVal;
    return xCallback(pCtx, &change);
  }


  if( pLeft->type==PROLLY_DIFF_MODIFY && pRight->type==PROLLY_DIFF_MODIFY ){
    change.pBaseVal = pLeft->pOldVal;
    change.nBaseVal = pLeft->nOldVal;
    change.pOurVal = pLeft->pNewVal;
    change.nOurVal = pLeft->nNewVal;
    change.pTheirVal = pRight->pNewVal;
    change.nTheirVal = pRight->nNewVal;
    if( valuesEqual(pLeft->pNewVal, pLeft->nNewVal,
                    pRight->pNewVal, pRight->nNewVal) ){
      change.type = THREE_WAY_CONVERGENT;
    }else{
      change.type = THREE_WAY_CONFLICT_MM;
    }
    return xCallback(pCtx, &change);
  }


  if( (pLeft->type==PROLLY_DIFF_DELETE && pRight->type==PROLLY_DIFF_MODIFY)
   || (pLeft->type==PROLLY_DIFF_MODIFY && pRight->type==PROLLY_DIFF_DELETE) ){
    change.type = THREE_WAY_CONFLICT_DM;
    change.pBaseVal = pLeft->pOldVal;
    change.nBaseVal = pLeft->nOldVal;
    if( pLeft->type==PROLLY_DIFF_MODIFY ){
      change.pOurVal = pLeft->pNewVal;
      change.nOurVal = pLeft->nNewVal;
    }else{
      change.pTheirVal = pRight->pNewVal;
      change.nTheirVal = pRight->nNewVal;
    }
    return xCallback(pCtx, &change);
  }


  change.type = THREE_WAY_CONFLICT_MM;
  change.pBaseVal = pLeft->pOldVal;
  change.nBaseVal = pLeft->nOldVal;
  change.pOurVal = pLeft->pNewVal;
  change.nOurVal = pLeft->nNewVal;
  change.pTheirVal = pRight->pNewVal;
  change.nTheirVal = pRight->nNewVal;
  return xCallback(pCtx, &change);
}

int prollyThreeWayDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pAncestorRoot,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffIter iterL;
  ProllyDiffIter iterR;
  ProllyDiffChange *pL = 0;
  ProllyDiffChange *pR = 0;
  int rcL, rcR;
  int rc = SQLITE_OK;

  rcL = prollyDiffIterOpen(&iterL, pStore, pCache,
                           pAncestorRoot, pOursRoot, flags);
  if( rcL!=SQLITE_OK ){
    rc = rcL;
    goto cleanup;
  }
  rcR = prollyDiffIterOpen(&iterR, pStore, pCache,
                           pAncestorRoot, pTheirsRoot, flags);
  if( rcR!=SQLITE_OK ){
    rc = rcR;
    goto cleanup;
  }

  /* Prime both iterators. */
  rcL = prollyDiffIterStep(&iterL, &pL);
  rcR = prollyDiffIterStep(&iterR, &pR);

  while( rcL==SQLITE_ROW && rcR==SQLITE_ROW ){
    int cmp = diffChangeKeyCmp(pL, pR, flags);
    if( cmp < 0 ){
      rc = emitLeftOnly(pL, xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      rcL = prollyDiffIterStep(&iterL, &pL);
    }else if( cmp > 0 ){
      rc = emitRightOnly(pR, xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      rcR = prollyDiffIterStep(&iterR, &pR);
    }else{
      rc = emitBothSides(pL, pR, xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      rcL = prollyDiffIterStep(&iterL, &pL);
      rcR = prollyDiffIterStep(&iterR, &pR);
    }
  }

  /* Drain whichever side has remaining entries. */
  while( rcL==SQLITE_ROW ){
    rc = emitLeftOnly(pL, xCallback, pCtx);
    if( rc!=SQLITE_OK ) goto cleanup;
    rcL = prollyDiffIterStep(&iterL, &pL);
  }
  while( rcR==SQLITE_ROW ){
    rc = emitRightOnly(pR, xCallback, pCtx);
    if( rc!=SQLITE_OK ) goto cleanup;
    rcR = prollyDiffIterStep(&iterR, &pR);
  }

  /* Propagate any iterator error (not SQLITE_DONE). */
  if( rcL!=SQLITE_DONE ) rc = rcL;
  if( rc==SQLITE_OK && rcR!=SQLITE_DONE ) rc = rcR;

cleanup:
  prollyDiffIterClose(&iterL);
  prollyDiffIterClose(&iterR);
  return rc;
}

#endif
