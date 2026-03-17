/*
** prolly_three_way_diff.c — Three-way row diff engine for prolly trees.
**
** Runs two prollyDiff passes (ancestor→ours, ancestor→theirs), collects
** both result streams into sorted arrays, then merge-walks them in key
** order to produce LEFT_*, RIGHT_*, CONVERGENT, or CONFLICT_* changes.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_diff.h"
#include <string.h>  /* memcmp, memcpy */

/*
** Internal structure for collecting diff changes into a dynamic array.
*/
typedef struct DiffEntry DiffEntry;
struct DiffEntry {
  u8 type;            /* PROLLY_DIFF_ADD, DELETE, or MODIFY */
  u8 *pKey;           /* Owned copy of key data */
  int nKey;
  i64 intKey;
  u8 *pOldVal;        /* Owned copy of old value */
  int nOldVal;
  u8 *pNewVal;        /* Owned copy of new value */
  int nNewVal;
};

typedef struct DiffCollector DiffCollector;
struct DiffCollector {
  DiffEntry *aEntry;  /* Array of collected entries */
  int nEntry;         /* Number of entries */
  int nAlloc;         /* Allocated capacity */
  u8 flags;           /* INTKEY or BLOBKEY */
};

/*
** Duplicate a blob. Returns NULL if pSrc is NULL or nSrc is 0.
*/
static u8 *dupBlob(const u8 *pSrc, int nSrc){
  u8 *pDst;
  if( !pSrc || nSrc<=0 ) return 0;
  pDst = (u8*)sqlite3_malloc(nSrc);
  if( pDst ) memcpy(pDst, pSrc, nSrc);
  return pDst;
}

/*
** Callback for prollyDiff — collects changes into the DiffCollector.
*/
static int diffCollectCallback(void *pCtx, const ProllyDiffChange *pChange){
  DiffCollector *pColl = (DiffCollector*)pCtx;
  DiffEntry *pEntry;

  /* Grow array if needed */
  if( pColl->nEntry >= pColl->nAlloc ){
    int nNew = pColl->nAlloc ? pColl->nAlloc*2 : 32;
    DiffEntry *aNew = (DiffEntry*)sqlite3_realloc(pColl->aEntry,
                                                   nNew*sizeof(DiffEntry));
    if( !aNew ) return SQLITE_NOMEM;
    pColl->aEntry = aNew;
    pColl->nAlloc = nNew;
  }

  pEntry = &pColl->aEntry[pColl->nEntry++];
  memset(pEntry, 0, sizeof(*pEntry));
  pEntry->type = pChange->type;
  pEntry->intKey = pChange->intKey;
  pEntry->pKey = dupBlob(pChange->pKey, pChange->nKey);
  pEntry->nKey = pChange->nKey;
  pEntry->pOldVal = dupBlob(pChange->pOldVal, pChange->nOldVal);
  pEntry->nOldVal = pChange->nOldVal;
  pEntry->pNewVal = dupBlob(pChange->pNewVal, pChange->nNewVal);
  pEntry->nNewVal = pChange->nNewVal;

  /* Check for allocation failure on non-null inputs */
  if( (pChange->pKey && pChange->nKey>0 && !pEntry->pKey)
   || (pChange->pOldVal && pChange->nOldVal>0 && !pEntry->pOldVal)
   || (pChange->pNewVal && pChange->nNewVal>0 && !pEntry->pNewVal) ){
    return SQLITE_NOMEM;
  }

  return SQLITE_OK;
}

/*
** Free all entries in a DiffCollector.
*/
static void diffCollectorFree(DiffCollector *pColl){
  int i;
  for(i=0; i<pColl->nEntry; i++){
    sqlite3_free(pColl->aEntry[i].pKey);
    sqlite3_free(pColl->aEntry[i].pOldVal);
    sqlite3_free(pColl->aEntry[i].pNewVal);
  }
  sqlite3_free(pColl->aEntry);
  pColl->aEntry = 0;
  pColl->nEntry = 0;
  pColl->nAlloc = 0;
}

/*
** Compare two DiffEntry keys for sorting/merging.
*/
static int diffEntryKeyCmp(const DiffEntry *pA, const DiffEntry *pB, u8 flags){
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

/*
** Compare two blobs for equality.
*/
static int blobsEqual(const u8 *pA, int nA, const u8 *pB, int nB){
  if( nA!=nB ) return 0;
  if( nA==0 ) return 1;
  return memcmp(pA, pB, nA)==0;
}

/*
** Fill a ThreeWayChange key from a DiffEntry.
*/
static void fillKeyFromEntry(ThreeWayChange *pOut, const DiffEntry *pEntry){
  pOut->pKey = pEntry->pKey;
  pOut->nKey = pEntry->nKey;
  pOut->intKey = pEntry->intKey;
}

/*
** Emit a left-only change (change exists in ours diff but not theirs).
*/
static int emitLeftOnly(
  const DiffEntry *pLeft,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromEntry(&change, pLeft);

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

/*
** Emit a right-only change (change exists in theirs diff but not ours).
*/
static int emitRightOnly(
  const DiffEntry *pRight,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromEntry(&change, pRight);

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

/*
** Emit a merged change where the same key appears in both diffs.
*/
static int emitBothSides(
  const DiffEntry *pLeft,
  const DiffEntry *pRight,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromEntry(&change, pLeft);

  /* Both ADD: check if they added the same value */
  if( pLeft->type==PROLLY_DIFF_ADD && pRight->type==PROLLY_DIFF_ADD ){
    if( blobsEqual(pLeft->pNewVal, pLeft->nNewVal,
                   pRight->pNewVal, pRight->nNewVal) ){
      change.type = THREE_WAY_CONVERGENT;
      change.pOurVal = pLeft->pNewVal;
      change.nOurVal = pLeft->nNewVal;
      change.pTheirVal = pRight->pNewVal;
      change.nTheirVal = pRight->nNewVal;
    }else{
      change.type = THREE_WAY_CONFLICT_MM;
      change.pOurVal = pLeft->pNewVal;
      change.nOurVal = pLeft->nNewVal;
      change.pTheirVal = pRight->pNewVal;
      change.nTheirVal = pRight->nNewVal;
    }
    return xCallback(pCtx, &change);
  }

  /* Both DELETE: convergent */
  if( pLeft->type==PROLLY_DIFF_DELETE && pRight->type==PROLLY_DIFF_DELETE ){
    change.type = THREE_WAY_CONVERGENT;
    change.pBaseVal = pLeft->pOldVal;
    change.nBaseVal = pLeft->nOldVal;
    return xCallback(pCtx, &change);
  }

  /* Both MODIFY: check if they modified to the same value */
  if( pLeft->type==PROLLY_DIFF_MODIFY && pRight->type==PROLLY_DIFF_MODIFY ){
    change.pBaseVal = pLeft->pOldVal;
    change.nBaseVal = pLeft->nOldVal;
    change.pOurVal = pLeft->pNewVal;
    change.nOurVal = pLeft->nNewVal;
    change.pTheirVal = pRight->pNewVal;
    change.nTheirVal = pRight->nNewVal;
    if( blobsEqual(pLeft->pNewVal, pLeft->nNewVal,
                   pRight->pNewVal, pRight->nNewVal) ){
      change.type = THREE_WAY_CONVERGENT;
    }else{
      change.type = THREE_WAY_CONFLICT_MM;
    }
    return xCallback(pCtx, &change);
  }

  /* One deleted, the other modified: CONFLICT_DM */
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

  /* One added, the other deleted/modified — shouldn't normally happen
  ** with a correct ancestor, but handle gracefully as a conflict. */
  change.type = THREE_WAY_CONFLICT_MM;
  change.pBaseVal = pLeft->pOldVal;
  change.nBaseVal = pLeft->nOldVal;
  change.pOurVal = pLeft->pNewVal;
  change.nOurVal = pLeft->nNewVal;
  change.pTheirVal = pRight->pNewVal;
  change.nTheirVal = pRight->nNewVal;
  return xCallback(pCtx, &change);
}

/*
** Compute three-way diff between ancestor, ours, and theirs trees.
**
** Algorithm:
** 1. prollyDiff(ancestor, ours) → collect left changes
** 2. prollyDiff(ancestor, theirs) → collect right changes
** 3. Merge-walk both arrays in sorted key order
*/
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
  DiffCollector left;
  DiffCollector right;
  int rc;
  int iL, iR;

  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  left.flags = flags;
  right.flags = flags;

  /* Step 1: Diff ancestor→ours */
  rc = prollyDiff(pStore, pCache, pAncestorRoot, pOursRoot,
                  flags, diffCollectCallback, &left);
  if( rc!=SQLITE_OK ) goto cleanup;

  /* Step 2: Diff ancestor→theirs */
  rc = prollyDiff(pStore, pCache, pAncestorRoot, pTheirsRoot,
                  flags, diffCollectCallback, &right);
  if( rc!=SQLITE_OK ) goto cleanup;

  /* Step 3: Merge-walk both sorted diff streams.
  ** prollyDiff already produces entries in key order because it walks
  ** cursors in sorted order. No additional sorting needed. */
  iL = 0;
  iR = 0;
  while( iL < left.nEntry && iR < right.nEntry ){
    int cmp = diffEntryKeyCmp(&left.aEntry[iL], &right.aEntry[iR], flags);
    if( cmp < 0 ){
      /* Key only in left diff */
      rc = emitLeftOnly(&left.aEntry[iL], xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iL++;
    }else if( cmp > 0 ){
      /* Key only in right diff */
      rc = emitRightOnly(&right.aEntry[iR], xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iR++;
    }else{
      /* Same key in both diffs */
      rc = emitBothSides(&left.aEntry[iL], &right.aEntry[iR],
                         xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iL++;
      iR++;
    }
  }

  /* Drain remaining left entries */
  while( iL < left.nEntry ){
    rc = emitLeftOnly(&left.aEntry[iL], xCallback, pCtx);
    if( rc!=SQLITE_OK ) goto cleanup;
    iL++;
  }

  /* Drain remaining right entries */
  while( iR < right.nEntry ){
    rc = emitRightOnly(&right.aEntry[iR], xCallback, pCtx);
    if( rc!=SQLITE_OK ) goto cleanup;
    iR++;
  }

cleanup:
  diffCollectorFree(&left);
  diffCollectorFree(&right);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
