
#ifdef DOLTLITE_PROLLY

#include "prolly_three_way_diff.h"
#include <string.h>  

typedef struct DiffEntry DiffEntry;
struct DiffEntry {
  u8 type;            
  u8 *pKey;           
  int nKey;
  i64 intKey;
  u8 *pOldVal;        
  int nOldVal;
  u8 *pNewVal;        
  int nNewVal;
};

typedef struct DiffCollector DiffCollector;
struct DiffCollector {
  DiffEntry *aEntry;  
  int nEntry;         
  int nAlloc;         
  u8 flags;           
};

static u8 *dupBlob(const u8 *pSrc, int nSrc){
  u8 *pDst;
  if( !pSrc || nSrc<=0 ) return 0;
  pDst = (u8*)sqlite3_malloc(nSrc);
  if( pDst ) memcpy(pDst, pSrc, nSrc);
  return pDst;
}

static int diffCollectCallback(void *pCtx, const ProllyDiffChange *pChange){
  DiffCollector *pColl = (DiffCollector*)pCtx;
  DiffEntry *pEntry;

  
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

  
  if( (pChange->pKey && pChange->nKey>0 && !pEntry->pKey)
   || (pChange->pOldVal && pChange->nOldVal>0 && !pEntry->pOldVal)
   || (pChange->pNewVal && pChange->nNewVal>0 && !pEntry->pNewVal) ){
    return SQLITE_NOMEM;
  }

  return SQLITE_OK;
}

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

static int valuesEqual(const u8 *pA, int nA, const u8 *pB, int nB){
  int equal = 0;
  return prollyValuesEqual(pA, nA, pB, nB, &equal)==SQLITE_OK && equal;
}

static void fillKeyFromEntry(ThreeWayChange *pOut, const DiffEntry *pEntry){
  pOut->pKey = pEntry->pKey;
  pOut->nKey = pEntry->nKey;
  pOut->intKey = pEntry->intKey;
}

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

static int emitBothSides(
  const DiffEntry *pLeft,
  const DiffEntry *pRight,
  ThreeWayDiffCallback xCallback,
  void *pCtx
){
  ThreeWayChange change;
  memset(&change, 0, sizeof(change));
  fillKeyFromEntry(&change, pLeft);

  
  if( pLeft->type==PROLLY_DIFF_ADD && pRight->type==PROLLY_DIFF_ADD ){
    if( valuesEqual(pLeft->pNewVal, pLeft->nNewVal,
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

/* Three-way diff: diff(ancestor,ours) and diff(ancestor,theirs) collected
** into sorted arrays, then merge-walked in key order. Same-key entries are
** classified as convergent (identical change), MM conflict (both modified
** differently), or DM conflict (one deleted, other modified). */
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

  
  rc = prollyDiff(pStore, pCache, pAncestorRoot, pOursRoot,
                  flags, diffCollectCallback, &left);
  if( rc!=SQLITE_OK ) goto cleanup;

  
  rc = prollyDiff(pStore, pCache, pAncestorRoot, pTheirsRoot,
                  flags, diffCollectCallback, &right);
  if( rc!=SQLITE_OK ) goto cleanup;

  
  iL = 0;
  iR = 0;
  while( iL < left.nEntry && iR < right.nEntry ){
    int cmp = diffEntryKeyCmp(&left.aEntry[iL], &right.aEntry[iR], flags);
    if( cmp < 0 ){
      
      rc = emitLeftOnly(&left.aEntry[iL], xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iL++;
    }else if( cmp > 0 ){
      
      rc = emitRightOnly(&right.aEntry[iR], xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iR++;
    }else{
      
      rc = emitBothSides(&left.aEntry[iL], &right.aEntry[iR],
                         xCallback, pCtx);
      if( rc!=SQLITE_OK ) goto cleanup;
      iL++;
      iR++;
    }
  }

  
  while( iL < left.nEntry ){
    rc = emitLeftOnly(&left.aEntry[iL], xCallback, pCtx);
    if( rc!=SQLITE_OK ) goto cleanup;
    iL++;
  }

  
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

#endif 
